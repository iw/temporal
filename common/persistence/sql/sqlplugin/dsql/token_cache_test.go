package dsql

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

// mockCredentialsProvider implements aws.CredentialsProvider for testing
type mockCredentialsProvider struct {
	retrieveFunc func(ctx context.Context) (aws.Credentials, error)
	callCount    atomic.Int32
}

func (m *mockCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	m.callCount.Add(1)
	if m.retrieveFunc != nil {
		return m.retrieveFunc(ctx)
	}
	return aws.Credentials{
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
	}, nil
}

func TestTokenCache_NewTokenCache(t *testing.T) {
	provider := &mockCredentialsProvider{}
	logger := log.NewNoopLogger()

	cache := NewTokenCache(provider, logger, nil)
	defer cache.Stop()

	require.NotNil(t, cache)
	assert.NotNil(t, cache.pools)
	assert.Equal(t, provider, cache.credentialsProvider)
	assert.Equal(t, logger, cache.logger)
	assert.Equal(t, 0, cache.Size())
	assert.Equal(t, DefaultPoolSize, cache.poolSize)
}

func TestPooledToken_IsValid(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		expiresAt time.Time
		expected  bool
	}{
		{
			name:      "valid token (expires in future)",
			expiresAt: now.Add(15 * time.Minute),
			expected:  true,
		},
		{
			name:      "expired token",
			expiresAt: now.Add(-5 * time.Minute),
			expected:  false,
		},
		{
			name:      "token expiring now",
			expiresAt: now,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := &pooledToken{
				token:     "test-token",
				expiresAt: tt.expiresAt,
			}
			result := token.isValid()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPooledToken_NeedsRefresh(t *testing.T) {
	now := time.Now()
	tokenDuration := 15 * time.Minute

	tests := []struct {
		name      string
		expiresAt time.Time
		expected  bool
	}{
		{
			name:      "fresh token (0% expired)",
			expiresAt: now.Add(15 * time.Minute),
			expected:  false,
		},
		{
			name:      "token at 50% lifetime",
			expiresAt: now.Add(7*time.Minute + 30*time.Second),
			expected:  false,
		},
		{
			name:      "token at 79% lifetime (just before refresh)",
			expiresAt: now.Add(3*time.Minute + 9*time.Second),
			expected:  false,
		},
		{
			name:      "token at 81% lifetime (should refresh)",
			expiresAt: now.Add(2*time.Minute + 51*time.Second),
			expected:  true,
		},
		{
			name:      "expired token",
			expiresAt: now.Add(-5 * time.Minute),
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := &pooledToken{
				token:     "test-token",
				expiresAt: tt.expiresAt,
			}
			result := token.needsRefresh(tokenDuration)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTokenCache_Clear(t *testing.T) {
	provider := &mockCredentialsProvider{}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)
	defer cache.Stop()

	// Add some tokens to the pools manually
	cache.mu.Lock()
	cache.pools[poolKey{host: "host1", region: "us-east-1", user: "admin", tokenDuration: 15 * time.Minute}] = &tokenPool{
		tokens: []*pooledToken{
			{token: "token1", expiresAt: time.Now().Add(15 * time.Minute)},
			{token: "token2", expiresAt: time.Now().Add(15 * time.Minute)},
		},
	}
	cache.pools[poolKey{host: "host2", region: "us-west-2", user: "admin", tokenDuration: 15 * time.Minute}] = &tokenPool{
		tokens: []*pooledToken{
			{token: "token3", expiresAt: time.Now().Add(15 * time.Minute)},
		},
	}
	cache.mu.Unlock()

	assert.Equal(t, 3, cache.Size())

	cache.Clear()

	assert.Equal(t, 0, cache.Size())
}

func TestTokenCache_Size(t *testing.T) {
	provider := &mockCredentialsProvider{}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)
	defer cache.Stop()

	assert.Equal(t, 0, cache.Size())

	// Add tokens manually
	cache.mu.Lock()
	cache.pools[poolKey{host: "host1", region: "us-east-1", user: "admin", tokenDuration: 15 * time.Minute}] = &tokenPool{
		tokens: []*pooledToken{
			{token: "token1", expiresAt: time.Now().Add(15 * time.Minute)},
		},
	}
	cache.mu.Unlock()

	assert.Equal(t, 1, cache.Size())

	cache.mu.Lock()
	cache.pools[poolKey{host: "host2", region: "us-west-2", user: "admin", tokenDuration: 15 * time.Minute}] = &tokenPool{
		tokens: []*pooledToken{
			{token: "token2", expiresAt: time.Now().Add(15 * time.Minute)},
			{token: "token3", expiresAt: time.Now().Add(15 * time.Minute)},
		},
	}
	cache.mu.Unlock()

	assert.Equal(t, 3, cache.Size())
}

func TestPoolKey_Uniqueness(t *testing.T) {
	// Test that different configurations produce different pool keys
	key1 := poolKey{host: "host1.dsql.us-east-1.on.aws", region: "us-east-1", user: "admin", tokenDuration: 15 * time.Minute}
	key2 := poolKey{host: "host2.dsql.us-east-1.on.aws", region: "us-east-1", user: "admin", tokenDuration: 15 * time.Minute}
	key3 := poolKey{host: "host1.dsql.us-east-1.on.aws", region: "us-west-2", user: "admin", tokenDuration: 15 * time.Minute}
	key4 := poolKey{host: "host1.dsql.us-east-1.on.aws", region: "us-east-1", user: "custom", tokenDuration: 15 * time.Minute}
	key5 := poolKey{host: "host1.dsql.us-east-1.on.aws", region: "us-east-1", user: "admin", tokenDuration: 30 * time.Minute}

	// All keys should be different
	assert.NotEqual(t, key1, key2, "different hosts should produce different keys")
	assert.NotEqual(t, key1, key3, "different regions should produce different keys")
	assert.NotEqual(t, key1, key4, "different users should produce different keys")
	assert.NotEqual(t, key1, key5, "different durations should produce different keys")

	// Same configuration should produce same key
	key1Copy := poolKey{host: "host1.dsql.us-east-1.on.aws", region: "us-east-1", user: "admin", tokenDuration: 15 * time.Minute}
	assert.Equal(t, key1, key1Copy, "same configuration should produce same key")
}

func TestTokenCache_ConcurrentAccess(t *testing.T) {
	provider := &mockCredentialsProvider{}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)
	defer cache.Stop()

	// Pre-populate pool with valid tokens
	cache.mu.Lock()
	cache.pools[poolKey{host: "test.dsql.us-east-1.on.aws", region: "us-east-1", user: "admin", tokenDuration: 15 * time.Minute}] = &tokenPool{
		tokens: []*pooledToken{
			{token: "cached-token-1", expiresAt: time.Now().Add(15 * time.Minute)},
			{token: "cached-token-2", expiresAt: time.Now().Add(15 * time.Minute)},
			{token: "cached-token-3", expiresAt: time.Now().Add(15 * time.Minute)},
		},
	}
	cache.mu.Unlock()

	// Run concurrent reads
	var wg sync.WaitGroup
	numGoroutines := 100
	results := make([]string, numGoroutines)
	errs := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			token, err := cache.GetToken(context.Background(), "test.dsql.us-east-1.on.aws", "us-east-1", "admin", 15*time.Minute)
			results[idx] = token
			errs[idx] = err
		}(i)
	}

	wg.Wait()

	// All goroutines should have gotten a valid token
	for i := 0; i < numGoroutines; i++ {
		assert.NoError(t, errs[i])
		assert.Contains(t, []string{"cached-token-1", "cached-token-2", "cached-token-3"}, results[i])
	}
}

func TestTokenCache_ConcurrentClearAndAccess(t *testing.T) {
	provider := &mockCredentialsProvider{}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)
	defer cache.Stop()

	// Pre-populate pool
	cache.mu.Lock()
	cache.pools[poolKey{host: "test.dsql.us-east-1.on.aws", region: "us-east-1", user: "admin", tokenDuration: 15 * time.Minute}] = &tokenPool{
		tokens: []*pooledToken{
			{token: "cached-token", expiresAt: time.Now().Add(15 * time.Minute)},
		},
	}
	cache.mu.Unlock()

	var wg sync.WaitGroup

	// Start readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = cache.Size()
			}
		}()
	}

	// Start clearers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				cache.Clear()
			}
		}()
	}

	// Should complete without deadlock or panic
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out - possible deadlock")
	}
}

func TestDefaultPoolSize(t *testing.T) {
	assert.Equal(t, 3, DefaultPoolSize, "DefaultPoolSize should be 3")
}

func TestRefreshAtLifetimePercent(t *testing.T) {
	assert.Equal(t, 0.8, RefreshAtLifetimePercent, "RefreshAtLifetimePercent should be 0.8 (80%)")
}

func TestDefaultTokenDuration(t *testing.T) {
	assert.Equal(t, 15*time.Minute, DefaultTokenDuration, "DefaultTokenDuration should be 15 minutes")
}

func TestAdminUser(t *testing.T) {
	assert.Equal(t, "admin", adminUser, "adminUser should be 'admin'")
}

func TestTokenCache_Stop(t *testing.T) {
	provider := &mockCredentialsProvider{}
	cache := NewTokenCache(provider, log.NewNoopLogger(), nil)

	// Stop should not panic and should be idempotent
	cache.Stop()
	cache.Stop() // Second call should be safe
}
