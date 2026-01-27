package dsql

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

// PoolKeeperConfig configures the pool keeper behavior.
type PoolKeeperConfig struct {
	// TargetPoolSize is the desired number of open connections.
	TargetPoolSize int

	// TickInterval is how often the keeper checks pool size.
	// Default: 1 second
	TickInterval time.Duration

	// MaxConnsPerTick limits how many connections to create per tick.
	// Calculated dynamically based on pool size and stagger duration.
	// Default: calculated to handle peak expiry rate within rate limits
	MaxConnsPerTick int

	// ConnectionTimeout is the timeout for creating a single connection.
	// Default: 10 seconds
	ConnectionTimeout time.Duration
}

// Environment variable names for pool keeper configuration
const (
	PoolKeeperTickIntervalEnvVar = "DSQL_POOL_KEEPER_TICK_INTERVAL"
	PoolKeeperMaxConnsEnvVar     = "DSQL_POOL_KEEPER_MAX_CONNS_PER_TICK"
)

// consecutiveFailuresBeforeStop is the number of consecutive "database is closed"
// errors before the keeper stops. This prevents premature shutdown during startup
// when temporary pools may be created and closed. Set high enough to survive
// Temporal's initialization phase where multiple temporary pools are created.
const consecutiveFailuresBeforeStop = 10

// DefaultPoolKeeperConfig returns configuration scaled for the pool size.
// For large pools (500+), MaxConnsPerTick is calculated to handle peak
// expiry rates while respecting the DSQL cluster rate limit.
func DefaultPoolKeeperConfig(targetPoolSize int) PoolKeeperConfig {
	tickInterval := getEnvDuration(PoolKeeperTickIntervalEnvVar, 1*time.Second)

	// Calculate MaxConnsPerTick based on pool size and stagger duration.
	// With 2-minute stagger, connections expire over a 2-minute window.
	// We need to be able to refill at the peak expiry rate.
	//
	// For small pools (≤100): 1 per tick is sufficient
	// For large pools: scale up to handle peak expiry
	//
	// Example: 500 connections, 2-min stagger, 1-sec tick
	//   - Peak expiry: 500 / 120 = ~4.2/sec
	//   - MaxConnsPerTick = 5 (rounds up)
	//
	// The rate limiter (local or DynamoDB) will throttle if we exceed
	// the cluster-wide limit, so this is just the local desire.
	staggerDuration := 2 * time.Minute // matches DefaultPoolWarmupConfig
	checksPerStagger := max(int(staggerDuration/tickInterval), 1)

	maxConnsPerTick := max((targetPoolSize+checksPerStagger-1)/checksPerStagger, 1) // ceil division
	// Cap at a reasonable maximum to avoid overwhelming the rate limiter
	// Even with DynamoDB coordination, we don't want one service hogging budget
	maxConnsPerTick = min(maxConnsPerTick, 10)

	// Allow env override for tuning
	if envMax := getEnvInt(PoolKeeperMaxConnsEnvVar, 0); envMax > 0 {
		maxConnsPerTick = envMax
	}

	return PoolKeeperConfig{
		TargetPoolSize:    targetPoolSize,
		TickInterval:      tickInterval,
		MaxConnsPerTick:   maxConnsPerTick,
		ConnectionTimeout: 10 * time.Second,
	}
}

// PoolKeeper maintains pool size by topping up when connections are closed.
// It uses an edge-triggered approach: check every tick, create at most K connections.
// This provides smooth convergence without bursts.
type PoolKeeper struct {
	db     *sql.DB
	cfg    PoolKeeperConfig
	logger log.Logger

	// Metrics
	topUpCounter  metrics.CounterIface
	topUpFailures metrics.CounterIface
	poolSizeGauge metrics.GaugeIface

	// Lifecycle
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Stats
	mu                        sync.Mutex
	totalTopUps               int64
	totalFailures             int64
	consecutiveClosedFailures int // Track consecutive "database is closed" errors
}

// NewPoolKeeper creates a new pool keeper.
func NewPoolKeeper(
	db *sql.DB,
	cfg PoolKeeperConfig,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *PoolKeeper {
	pk := &PoolKeeper{
		db:     db,
		cfg:    cfg,
		logger: logger,
	}

	if metricsHandler != nil {
		pk.topUpCounter = metricsHandler.Counter("dsql_pool_keeper_topup_total")
		pk.topUpFailures = metricsHandler.Counter("dsql_pool_keeper_topup_failures_total")
		pk.poolSizeGauge = metricsHandler.Gauge("dsql_pool_keeper_current_size")
	}

	return pk
}

// Start begins the keeper loop.
func (pk *PoolKeeper) Start(ctx context.Context) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	pk.cancel = cancel

	pk.wg.Add(1)
	go pk.keeperLoop(ctx)

	if pk.logger != nil {
		pk.logger.Info("DSQL pool keeper started",
			tag.NewInt("target_pool_size", pk.cfg.TargetPoolSize),
			tag.NewDurationTag("tick_interval", pk.cfg.TickInterval),
			tag.NewInt("max_conns_per_tick", pk.cfg.MaxConnsPerTick))
	}

	return cancel
}

// Stop stops the keeper loop and waits for it to finish.
func (pk *PoolKeeper) Stop() {
	if pk.cancel != nil {
		pk.cancel()
	}
	pk.wg.Wait()
}

// keeperLoop is the main loop that maintains pool size.
func (pk *PoolKeeper) keeperLoop(ctx context.Context) {
	defer pk.wg.Done()

	ticker := time.NewTicker(pk.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			pk.mu.Lock()
			topUps := pk.totalTopUps
			failures := pk.totalFailures
			pk.mu.Unlock()

			if pk.logger != nil {
				pk.logger.Info("DSQL pool keeper stopped",
					tag.NewInt64("total_topups", topUps),
					tag.NewInt64("total_failures", failures))
			}
			return

		case <-ticker.C:
			pk.tick(ctx)
		}
	}
}

// tick checks pool size and tops up if needed.
func (pk *PoolKeeper) tick(ctx context.Context) {
	stats := pk.db.Stats()

	// Check if database is closed by looking at MaxOpenConnections.
	// When a database is closed, Stats() returns zeros.
	// However, we don't stop immediately - we wait for consecutive failures
	// to avoid premature shutdown during startup.
	if stats.MaxOpenConnections == 0 {
		pk.mu.Lock()
		pk.consecutiveClosedFailures++
		failures := pk.consecutiveClosedFailures
		pk.mu.Unlock()

		if failures >= consecutiveFailuresBeforeStop {
			if pk.logger != nil {
				pk.logger.Info("Pool keeper detected closed database (MaxOpenConnections=0), stopping",
					tag.NewInt("consecutive_failures", failures))
			}
			if pk.cancel != nil {
				pk.cancel()
			}
		}
		return
	}

	// Reset consecutive failures counter when we see a healthy pool
	pk.mu.Lock()
	pk.consecutiveClosedFailures = 0
	pk.mu.Unlock()

	currentOpen := stats.OpenConnections

	// Record current size
	if pk.poolSizeGauge != nil {
		pk.poolSizeGauge.Record(float64(currentOpen))
	}

	// Check if we need to top up
	deficit := pk.cfg.TargetPoolSize - currentOpen
	if deficit <= 0 {
		return // Pool is at or above target
	}

	// Create at most MaxConnsPerTick connections this tick
	toCreate := min(deficit, pk.cfg.MaxConnsPerTick)

	// Edge-triggered: create connections one at a time with rate limiting
	for i := 0; i < toCreate; i++ {
		if ctx.Err() != nil {
			return
		}

		if pk.createOneConnection(ctx) {
			pk.mu.Lock()
			pk.totalTopUps++
			pk.consecutiveClosedFailures = 0 // Reset on success
			pk.mu.Unlock()

			if pk.topUpCounter != nil {
				pk.topUpCounter.Record(1)
			}
		} else {
			pk.mu.Lock()
			pk.totalFailures++
			pk.mu.Unlock()

			if pk.topUpFailures != nil {
				pk.topUpFailures.Record(1)
			}
		}
	}
}

// createOneConnection creates a single connection and returns it to the pool.
func (pk *PoolKeeper) createOneConnection(ctx context.Context) bool {
	connCtx, cancel := context.WithTimeout(ctx, pk.cfg.ConnectionTimeout)
	defer cancel()

	conn, err := pk.db.Conn(connCtx)
	if err != nil {
		// Check for "database is closed" error - track consecutive failures
		if isDatabaseClosedError(err) {
			pk.mu.Lock()
			pk.consecutiveClosedFailures++
			failures := pk.consecutiveClosedFailures
			pk.mu.Unlock()

			if failures >= consecutiveFailuresBeforeStop {
				if pk.logger != nil {
					pk.logger.Info("Pool keeper detected closed database, stopping",
						tag.NewInt("consecutive_failures", failures))
				}
				if pk.cancel != nil {
					pk.cancel()
				}
			}
			return false
		}

		if pk.logger != nil {
			pk.logger.Warn("Pool keeper failed to create connection",
				tag.Error(err))
		}
		return false
	}

	// Verify connection is working
	var result int
	err = conn.QueryRowContext(connCtx, "SELECT 1").Scan(&result)
	if err != nil {
		conn.Close()
		if pk.logger != nil {
			pk.logger.Warn("Pool keeper connection verification failed",
				tag.Error(err))
		}
		return false
	}

	// Return to pool
	conn.Close()
	return true
}

// isDatabaseClosedError checks if the error indicates the database is closed.
func isDatabaseClosedError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "sql: database is closed" || strings.Contains(errStr, "database is closed")
}

// Stats returns keeper statistics.
func (pk *PoolKeeper) Stats() (totalTopUps, totalFailures int64) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	return pk.totalTopUps, pk.totalFailures
}
