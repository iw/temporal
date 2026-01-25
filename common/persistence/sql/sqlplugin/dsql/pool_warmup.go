package dsql

import (
	"context"
	"database/sql"
	"math/rand"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/session"
)

// PoolWarmupConfig configures the connection pool pre-warming behavior.
type PoolWarmupConfig struct {
	// Enabled controls whether pool pre-warming is enabled.
	// Default: true
	Enabled bool

	// TargetConnections is the number of connections to pre-warm.
	// If 0, defaults to session.DefaultMaxConns (100).
	// Note: plugin.go overrides this with the actual configured pool size.
	TargetConnections int

	// MaxRetries is the maximum number of retry attempts per connection.
	// Default: 5
	MaxRetries int

	// RetryBackoff is the initial backoff duration between retries.
	// Doubles with each retry attempt (exponential backoff).
	// Default: 200ms
	RetryBackoff time.Duration

	// MaxBackoff is the maximum backoff duration between retries.
	// Default: 5 seconds
	MaxBackoff time.Duration

	// ConnectionTimeout is the timeout for each individual connection attempt.
	// Default: 10 seconds
	ConnectionTimeout time.Duration

	// StaggerDuration is the total time window over which to spread connection creation.
	// Connections are created with delays spread across this window to ensure they
	// hit MaxConnLifetime at different times, avoiding thundering herd on expiry.
	//
	// For large pools, this is calculated based on the DSQL cluster rate limit (100/sec)
	// to ensure warmup doesn't exceed the rate limit.
	//
	// Default: calculated based on pool size
	//   - Small pools (≤100): 0 (no stagger, fast warmup)
	//   - Medium pools (101-500): 2 minutes
	//   - Large pools (500+): scaled to respect rate limit with 50% headroom
	//
	// Set to 0 to disable staggering (all connections created immediately).
	StaggerDuration time.Duration
}

// Environment variable names for pool warmup configuration
const (
	PoolWarmupStaggerDurationEnvVar = "DSQL_POOL_WARMUP_STAGGER_DURATION"
)

// ClusterRateLimitForWarmup is the portion of cluster rate limit to use during warmup.
// We use 50% to leave headroom for other services warming up simultaneously.
const ClusterRateLimitForWarmup = 50 // connections per second (50% of 100)

// DefaultPoolWarmupConfig returns the default pool warmup configuration.
// For large pools, stagger duration is calculated to respect DSQL rate limits.
func DefaultPoolWarmupConfig() PoolWarmupConfig {
	return PoolWarmupConfig{
		Enabled:           true,
		TargetConnections: session.DefaultMaxConns, // 100 (overridden by plugin.go)
		MaxRetries:        5,
		RetryBackoff:      200 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		ConnectionTimeout: 10 * time.Second,
		StaggerDuration:   0, // Calculated in CalculateStaggerDuration based on pool size
	}
}

// CalculateStaggerDuration returns the appropriate stagger duration for a pool size.
// This ensures connections are spread out to avoid thundering herd on expiry,
// while respecting the DSQL cluster-wide rate limit during warmup.
func CalculateStaggerDuration(poolSize int) time.Duration {
	// Allow env override for tuning
	if envStagger := getEnvDuration(PoolWarmupStaggerDurationEnvVar, 0); envStagger > 0 {
		return envStagger
	}

	// Small pools: no stagger needed, fast warmup
	if poolSize <= 100 {
		return 0
	}

	// Medium pools: 2 minutes is sufficient
	if poolSize <= 500 {
		return 2 * time.Minute
	}

	// Large pools: calculate based on rate limit
	// We want warmup to complete within rate limit, with 50% headroom for other services
	//
	// Example: 8000 connections at 50/sec = 160 seconds = 2.67 minutes
	// Round up to nearest minute for cleaner numbers
	minSeconds := poolSize / ClusterRateLimitForWarmup
	minDuration := time.Duration(minSeconds) * time.Second

	// Round up to nearest minute, minimum 3 minutes for large pools
	minutes := (int(minDuration.Minutes()) + 1)
	if minutes < 3 {
		minutes = 3
	}

	return time.Duration(minutes) * time.Minute
}

// WarmupResult contains the result of pool warmup including connections for tracking.
type WarmupResult struct {
	// Connections holds the warmed connections.
	// The caller should close these to return them to the pool.
	Connections []*sql.Conn

	// SuccessCount is the number of connections successfully created.
	SuccessCount int

	// FailedCount is the number of connections that failed to create.
	FailedCount int

	// TotalRetries is the total number of retry attempts across all connections.
	TotalRetries int
}

// WarmupPool pre-warms the connection pool by establishing connections up front.
// This avoids cold-start latency when load arrives.
//
// The warmup process:
// 1. Acquires connections up to the target count, holding them all open
// 2. Executes a simple query on each to ensure they're fully established
// 3. Returns the connections for lifecycle tracking (caller must close them)
//
// By holding all connections open during warmup, we ensure they stay in the pool
// rather than being closed by Go's database/sql idle connection management.
//
// The returned WarmupResult.Connections should be closed by the caller
// to return them to the pool.
func WarmupPool(ctx context.Context, db *sql.DB, cfg PoolWarmupConfig, logger log.Logger) (*WarmupResult, error) {
	if !cfg.Enabled {
		if logger != nil {
			logger.Info("DSQL pool warmup disabled")
		}
		return &WarmupResult{}, nil
	}

	// Determine target connections
	stats := db.Stats()
	targetConns := cfg.TargetConnections
	if targetConns <= 0 {
		targetConns = session.DefaultMaxConns
	}

	// Don't warm up more than MaxOpenConnections if it's set
	if stats.MaxOpenConnections > 0 && targetConns > stats.MaxOpenConnections {
		targetConns = stats.MaxOpenConnections
	}

	// Calculate how many new connections we need
	currentOpen := stats.OpenConnections
	needed := targetConns - currentOpen
	if needed <= 0 {
		if logger != nil {
			logger.Info("DSQL pool already warm",
				tag.NewInt("current_connections", currentOpen),
				tag.NewInt("target_connections", targetConns))
		}
		return &WarmupResult{}, nil
	}

	if logger != nil {
		logger.Info("Starting DSQL pool warmup",
			tag.NewInt("current_connections", currentOpen),
			tag.NewInt("target_connections", targetConns),
			tag.NewInt("connections_to_create", needed),
			tag.NewInt("max_retries", cfg.MaxRetries),
			tag.NewDurationTag("stagger_duration", cfg.StaggerDuration))
	}

	startTime := time.Now()

	// Hold all connections open during warmup to prevent them from being closed
	connections := make([]*sql.Conn, 0, needed)

	successCount := 0
	totalRetries := 0

	// Calculate delay between connections to spread them across StaggerDuration.
	// This ensures connections hit MaxConnLifetime at different times.
	var delayBetweenConns time.Duration
	if cfg.StaggerDuration > 0 && needed > 1 {
		delayBetweenConns = cfg.StaggerDuration / time.Duration(needed-1)
	}

	// Create connections sequentially with staggered delays
	for i := 0; i < needed; i++ {
		// Check if parent context is cancelled
		if ctx.Err() != nil {
			break
		}

		// Add staggered delay (except for first connection)
		if i > 0 && delayBetweenConns > 0 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(delayBetweenConns):
				// Continue to create next connection
			}
		}

		conn, retries, success := warmupOneConnectionWithRetry(ctx, db, cfg, logger)
		totalRetries += retries
		if success && conn != nil {
			connections = append(connections, conn)
			successCount++
		}
	}

	elapsed := time.Since(startTime)
	finalStats := db.Stats()
	failed := needed - successCount

	if logger != nil {
		logger.Info("DSQL pool warmup complete",
			tag.NewInt("connections_created", successCount),
			tag.NewInt("connections_failed", failed),
			tag.NewInt("total_retries", totalRetries),
			tag.NewInt("final_open_connections", finalStats.OpenConnections),
			tag.NewInt("final_idle_connections", finalStats.Idle),
			tag.NewDurationTag("elapsed", elapsed))
	}

	// Return the connections - caller should close them to return to pool
	return &WarmupResult{
		Connections:  connections,
		SuccessCount: successCount,
		FailedCount:  failed,
		TotalRetries: totalRetries,
	}, nil
}

// warmupOneConnectionWithRetry establishes a single connection with retry logic.
// Returns (connection, retries_used, success). The caller is responsible for closing the connection.
func warmupOneConnectionWithRetry(ctx context.Context, db *sql.DB, cfg PoolWarmupConfig, logger log.Logger) (*sql.Conn, int, bool) {
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	backoff := cfg.RetryBackoff
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}

	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Second
	}

	connTimeout := cfg.ConnectionTimeout
	if connTimeout <= 0 {
		connTimeout = 10 * time.Second
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Check if parent context is cancelled
		if ctx.Err() != nil {
			return nil, attempt, false
		}

		// Wait before retry (not on first attempt)
		if attempt > 0 {
			// Add jitter: 50-150% of backoff to prevent thundering herd
			jitteredBackoff := time.Duration(float64(backoff) * (0.5 + rand.Float64()))
			select {
			case <-ctx.Done():
				return nil, attempt, false
			case <-time.After(jitteredBackoff):
				// Double backoff for next retry, capped at maxBackoff
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
		}

		// Create a timeout context for this specific connection attempt
		connCtx, cancel := context.WithTimeout(ctx, connTimeout)
		conn, err := warmupOneConnection(connCtx, db)
		cancel()

		if err == nil {
			return conn, attempt, true // Success - return the open connection
		}

		lastErr = err
		if logger != nil && attempt < maxRetries {
			logger.Debug("DSQL pool warmup connection failed, retrying",
				tag.NewInt("attempt", attempt+1),
				tag.NewInt("max_retries", maxRetries),
				tag.Error(err))
		}
	}

	// All retries exhausted
	if logger != nil {
		logger.Warn("DSQL pool warmup connection failed after all retries",
			tag.NewInt("retries", maxRetries),
			tag.Error(lastErr))
	}
	return nil, maxRetries, false
}

// warmupOneConnection establishes a single connection by executing a simple query.
// Returns the open connection - caller is responsible for closing it.
func warmupOneConnection(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// Execute a simple query to ensure the connection is fully established
	var result int
	err = conn.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// Return the open connection - caller will close it later
	return conn, nil
}

// StartPeriodicWarmup starts a background goroutine that periodically warms up the pool.
// This is useful because idle connections are closed after MaxConnIdleTime.
// Returns a cancel function to stop the periodic warmup.
//
// Note: This function closes warmed connections immediately after creation.
func StartPeriodicWarmup(
	ctx context.Context,
	db *sql.DB,
	cfg PoolWarmupConfig,
	interval time.Duration,
	logger log.Logger,
) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		// Initial warmup - close connections after warmup
		if result, err := WarmupPool(ctx, db, cfg, logger); err == nil && result != nil {
			for _, conn := range result.Connections {
				conn.Close()
			}
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if result, err := WarmupPool(ctx, db, cfg, logger); err == nil && result != nil {
					for _, conn := range result.Connections {
						conn.Close()
					}
				}
			}
		}
	}()

	return cancel
}
