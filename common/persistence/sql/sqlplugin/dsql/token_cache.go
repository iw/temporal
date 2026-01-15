// Token cache for DSQL IAM authentication tokens.
// Maintains cached tokens with proactive refresh to ensure connections always have valid tokens.

package dsql

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	// DefaultTokenDuration is the default token validity duration.
	// 15 minutes is the maximum allowed by Aurora DSQL.
	DefaultTokenDuration = 15 * time.Minute

	// MinTokenDuration is the minimum allowed token duration.
	MinTokenDuration = 15 * time.Second

	// RefreshAtLifetimePercent - refresh tokens when this much lifetime has passed
	RefreshAtLifetimePercent = 0.8

	// DefaultPoolSize is the default number of tokens to keep in the pool
	DefaultPoolSize = 3

	// adminUser is the DSQL admin user name
	adminUser = "admin"

	// TokenDurationEnvVar is the environment variable to override token duration.
	TokenDurationEnvVar = "DSQL_TOKEN_DURATION"

	// backgroundRefreshInterval is how often the background loop runs
	backgroundRefreshInterval = 10 * time.Second
)

// GetConfiguredTokenDuration returns the token duration from environment variable
// or the default if not set.
func GetConfiguredTokenDuration() time.Duration {
	if envDuration := os.Getenv(TokenDurationEnvVar); envDuration != "" {
		if d, err := time.ParseDuration(envDuration); err == nil {
			if d >= MinTokenDuration && d <= DefaultTokenDuration {
				return d
			}
		}
	}
	return DefaultTokenDuration
}

// poolKey uniquely identifies a token pool.
type poolKey struct {
	host          string
	region        string
	user          string
	tokenDuration time.Duration
}

// pooledToken holds a token with its expiration time.
type pooledToken struct {
	token     string
	expiresAt time.Time
}

// isValid returns true if the token hasn't expired.
func (pt *pooledToken) isValid() bool {
	return time.Now().Before(pt.expiresAt)
}

// needsRefresh returns true if the token is at or past 80% of its lifetime.
func (pt *pooledToken) needsRefresh(tokenDuration time.Duration) bool {
	refreshAt := pt.expiresAt.Add(-time.Duration(float64(tokenDuration) * (1 - RefreshAtLifetimePercent)))
	return time.Now().After(refreshAt)
}

// tokenPool holds multiple pre-generated tokens for a given key.
type tokenPool struct {
	tokens []*pooledToken
}

// TokenCache manages pools of pre-generated authentication tokens.
//
// # Design
//
// - Maintains a pool of pre-generated tokens (default 3 per key)
// - Background goroutine continuously refreshes tokens approaching expiry
// - GetToken() always returns immediately from the pool
// - If pool is empty (shouldn't happen), generates synchronously as fallback
//
// # Token Lifecycle
//
// 1. On first GetToken(), pool is initialized with tokens generated synchronously
// 2. Background loop runs every 10 seconds, checking all pools
// 3. Tokens at 80% lifetime are refreshed (new token generated, old one replaced)
// 4. GetToken() returns any valid token from the pool
type TokenCache struct {
	mu                  sync.RWMutex
	pools               map[poolKey]*tokenPool
	poolSize            int
	credentialsProvider aws.CredentialsProvider
	logger              log.Logger
	metricsHandler      metrics.Handler

	// Background refresh
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewTokenCache creates a new token cache with background refresh.
func NewTokenCache(
	credentialsProvider aws.CredentialsProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *TokenCache {
	tc := &TokenCache{
		pools:               make(map[poolKey]*tokenPool),
		poolSize:            DefaultPoolSize,
		credentialsProvider: credentialsProvider,
		logger:              logger,
		metricsHandler:      metricsHandler,
		stopCh:              make(chan struct{}),
	}

	// Start background refresh goroutine
	tc.wg.Add(1)
	go tc.backgroundRefreshLoop()

	return tc
}

// Stop stops the background refresh goroutine.
func (tc *TokenCache) Stop() {
	tc.stopOnce.Do(func() {
		close(tc.stopCh)
	})
	tc.wg.Wait()
}

// backgroundRefreshLoop continuously refreshes tokens approaching expiry.
func (tc *TokenCache) backgroundRefreshLoop() {
	defer tc.wg.Done()

	ticker := time.NewTicker(backgroundRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tc.stopCh:
			return
		case <-ticker.C:
			tc.refreshPools()
		}
	}
}

// refreshPools checks all pools and refreshes tokens that need it.
func (tc *TokenCache) refreshPools() {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	for key, pool := range tc.pools {
		tc.refreshPoolLocked(key, pool)
	}
}

// refreshPoolLocked refreshes tokens in a pool that need refreshing.
// Must be called with tc.mu held.
func (tc *TokenCache) refreshPoolLocked(key poolKey, pool *tokenPool) {
	// Remove expired tokens and refresh those approaching expiry
	validTokens := make([]*pooledToken, 0, tc.poolSize)

	for _, pt := range pool.tokens {
		if !pt.isValid() {
			// Token expired, discard it
			continue
		}
		if pt.needsRefresh(key.tokenDuration) {
			// Token approaching expiry, generate replacement
			newToken, err := tc.generateTokenLocked(key)
			if err != nil {
				tc.recordTokenRefreshFailure()
				if tc.logger != nil {
					tc.logger.Error("Background token refresh failed",
						tag.Error(err),
						tag.NewStringTag("host", key.host))
				}
				// Keep the old token for now
				validTokens = append(validTokens, pt)
			} else {
				validTokens = append(validTokens, newToken)
				tc.recordTokenRefresh()
				tc.logTokenRefresh(key.host, key.region, key.user, key.tokenDuration)
			}
		} else {
			// Token still good
			validTokens = append(validTokens, pt)
		}
	}

	// Ensure pool has enough tokens
	for len(validTokens) < tc.poolSize {
		newToken, err := tc.generateTokenLocked(key)
		if err != nil {
			tc.recordTokenRefreshFailure()
			if tc.logger != nil {
				tc.logger.Error("Failed to replenish token pool",
					tag.Error(err),
					tag.NewStringTag("host", key.host))
			}
			break
		}
		validTokens = append(validTokens, newToken)
		tc.recordTokenRefresh()
		tc.logTokenRefresh(key.host, key.region, key.user, key.tokenDuration)
	}

	pool.tokens = validTokens
}

// GetToken returns a valid token from the pool. On first call, initializes
// the pool synchronously. After that, always returns from pool immediately.
func (tc *TokenCache) GetToken(ctx context.Context, host, region, user string, tokenDuration time.Duration) (string, error) {
	if tokenDuration == 0 {
		tokenDuration = DefaultTokenDuration
	}

	key := poolKey{
		host:          host,
		region:        region,
		user:          user,
		tokenDuration: tokenDuration,
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	pool, exists := tc.pools[key]
	if !exists {
		// First request - initialize pool
		pool = &tokenPool{tokens: make([]*pooledToken, 0, tc.poolSize)}
		tc.pools[key] = pool

		// Generate initial tokens
		for i := 0; i < tc.poolSize; i++ {
			token, err := tc.generateTokenLocked(key)
			if err != nil {
				if i == 0 {
					// Can't even generate one token
					return "", err
				}
				// Got at least one, continue with what we have
				break
			}
			pool.tokens = append(pool.tokens, token)
			tc.recordTokenRefresh()
		}

		tc.logTokenRefresh(key.host, key.region, key.user, key.tokenDuration)
	}

	// Find a valid token in the pool
	for _, pt := range pool.tokens {
		if pt.isValid() {
			tc.recordCacheHit()
			return pt.token, nil
		}
	}

	// No valid tokens (shouldn't happen with background refresh)
	tc.recordCacheMiss()

	// Fallback: generate synchronously
	token, err := tc.generateTokenLocked(key)
	if err != nil {
		tc.recordTokenRefreshFailure()
		return "", err
	}

	pool.tokens = append(pool.tokens, token)
	tc.recordTokenRefresh()
	tc.logTokenRefresh(key.host, key.region, key.user, key.tokenDuration)

	return token.token, nil
}

// generateTokenLocked generates a new pooled token.
// Must be called with tc.mu held.
func (tc *TokenCache) generateTokenLocked(key poolKey) (*pooledToken, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var tokenOpts []func(*auth.TokenOptions)
	if key.tokenDuration > 0 {
		tokenOpts = append(tokenOpts, func(opts *auth.TokenOptions) {
			opts.ExpiresIn = key.tokenDuration
		})
	}

	var token string
	var err error

	if key.user == adminUser {
		token, err = auth.GenerateDBConnectAdminAuthToken(ctx, key.host, key.region, tc.credentialsProvider, tokenOpts...)
	} else {
		token, err = auth.GenerateDbConnectAuthToken(ctx, key.host, key.region, tc.credentialsProvider, tokenOpts...)
	}

	if err != nil {
		return nil, err
	}

	return &pooledToken{
		token:     token,
		expiresAt: time.Now().Add(key.tokenDuration),
	}, nil
}

// Clear removes all pools. Useful for testing.
func (tc *TokenCache) Clear() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.pools = make(map[poolKey]*tokenPool)
}

// Size returns total number of tokens across all pools.
func (tc *TokenCache) Size() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	count := 0
	for _, pool := range tc.pools {
		count += len(pool.tokens)
	}
	return count
}

// Metrics helpers
func (tc *TokenCache) recordCacheHit() {
	if tc.metricsHandler != nil {
		metrics.DSQLTokenCacheHits.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) recordCacheMiss() {
	if tc.metricsHandler != nil {
		metrics.DSQLTokenCacheMisses.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) recordTokenRefresh() {
	if tc.metricsHandler != nil {
		metrics.DSQLTokenRefreshes.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) recordTokenRefreshFailure() {
	if tc.metricsHandler != nil {
		metrics.DSQLTokenRefreshFailures.With(tc.metricsHandler).Record(1)
	}
}

func (tc *TokenCache) logTokenRefresh(host, region, user string, duration time.Duration) {
	if tc.logger != nil {
		tc.logger.Info("DSQL IAM token refreshed",
			tag.NewStringTag("host", host),
			tag.NewStringTag("region", region),
			tag.NewStringTag("user", user),
			tag.NewDurationTag("token_duration", duration))
	}
}

// ResolveCredentialsProvider resolves the AWS credentials provider once.
func ResolveCredentialsProvider(ctx context.Context, region string) (aws.CredentialsProvider, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return cfg.Credentials, nil
}
