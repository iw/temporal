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
	// If 0, defaults to session.DefaultMaxConns (50).
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
}

// DefaultPoolWarmupConfig returns the default pool warmup configuration.
func DefaultPoolWarmupConfig() PoolWarmupConfig {
	return PoolWarmupConfig{
		Enabled:           true,
		TargetConnections: session.DefaultMaxConns, // 50
		MaxRetries:        5,
		RetryBackoff:      200 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		ConnectionTimeout: 10 * time.Second,
	}
}

// WarmupPool pre-warms the connection pool by establishing connections up front.
// This avoids cold-start latency when load arrives.
//
// The warmup process:
// 1. Acquires connections up to the target count, holding them all open
// 2. Executes a simple query on each to ensure they're fully established
// 3. Releases all connections back to the pool at once
//
// By holding all connections open during warmup, we ensure they stay in the pool
// rather than being closed by Go's database/sql idle connection management.
func WarmupPool(ctx context.Context, db *sql.DB, cfg PoolWarmupConfig, logger log.Logger) error {
	if !cfg.Enabled {
		if logger != nil {
			logger.Info("DSQL pool warmup disabled")
		}
		return nil
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
		return nil
	}

	if logger != nil {
		logger.Info("Starting DSQL pool warmup",
			tag.NewInt("current_connections", currentOpen),
			tag.NewInt("target_connections", targetConns),
			tag.NewInt("connections_to_create", needed),
			tag.NewInt("max_retries", cfg.MaxRetries))
	}

	startTime := time.Now()

	// Hold all connections open during warmup to prevent them from being closed
	connections := make([]*sql.Conn, 0, needed)
	defer func() {
		// Release all connections back to the pool
		for _, conn := range connections {
			conn.Close()
		}
	}()

	successCount := 0
	totalRetries := 0

	// Create connections sequentially with retries, holding each one open
	for i := 0; i < needed; i++ {
		// Check if parent context is cancelled
		if ctx.Err() != nil {
			break
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

	// Connections are released when the defer runs, returning them to the pool
	return nil
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
func StartPeriodicWarmup(
	ctx context.Context,
	db *sql.DB,
	cfg PoolWarmupConfig,
	interval time.Duration,
	logger log.Logger,
) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		// Initial warmup
		_ = WarmupPool(ctx, db, cfg, logger)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = WarmupPool(ctx, db, cfg, logger)
			}
		}
	}()

	return cancel
}
