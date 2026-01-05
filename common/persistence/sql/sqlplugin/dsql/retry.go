// Package dsql provides Aurora DSQL-specific helpers for Temporal's SQL persistence plugin.
//
// This file implements transaction retry semantics suitable for Aurora DSQL's
// optimistic concurrency control (OCC) model.
//
// Authoritative retry rules (DSQL plugin):
//   1) ConditionFailedError (CAS / fencing loss) => NEVER retry, return immediately.
//   2) SQLSTATE 40001 (serialization failure)     => retry entire transaction.
//   3) SQLSTATE 0A000 (feature not supported)     => fail fast (implementation bug), no retry.
//   4) All others                                 => permanent failure, no retry.
//
// Notes:
// - Aurora DSQL does not block on locks; conflicting transactions typically fail at commit.
// - Callers must implement CAS via conditional updates (rowsAffected == 0 => ConditionFailedError).
package dsql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

//
// Configuration
//

// RetryConfig contains configuration for DSQL retry behavior.
type RetryConfig struct {
	MaxRetries   int           `yaml:"max_retries" json:"max_retries"`
	BaseDelay    time.Duration `yaml:"base_delay" json:"base_delay"`
	MaxDelay     time.Duration `yaml:"max_delay" json:"max_delay"`
	JitterFactor float64       `yaml:"jitter_factor" json:"jitter_factor"`
}

// DefaultRetryConfig returns a conservative default retry configuration for DSQL.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   5,
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     5 * time.Second,
		JitterFactor: 0.25,
	}
}

func (c RetryConfig) validate() RetryConfig {
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = 100 * time.Millisecond
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 5 * time.Second
	}
	if c.MaxDelay < c.BaseDelay {
		c.MaxDelay = c.BaseDelay
	}
	if c.JitterFactor < 0 {
		c.JitterFactor = 0
	}
	// Reasonable upper bound; >1 produces negative delays too often.
	if c.JitterFactor > 1.0 {
		c.JitterFactor = 1.0
	}
	return c
}

//
// Error types are defined in types.go
//

//
// RetryManager
//

// RetryManager manages retry logic for DSQL operations.
type RetryManager struct {
	db      *sql.DB
	config  RetryConfig
	logger  log.Logger
	metrics DSQLMetrics
	rng     *rand.Rand
}

// NewRetryManager creates a new RetryManager.
// metricsHandler may be nil; metrics may be nil if you haven't implemented them yet.
func NewRetryManager(
	db *sql.DB,
	config RetryConfig,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *RetryManager {
	config = config.validate()

	var m DSQLMetrics
	m = NewDSQLMetrics(metricsHandler)

	return &RetryManager{
		db:      db,
		config:  config,
		logger:  logger,
		metrics: m,
		// Per-manager RNG to avoid global lock contention and to allow deterministic tests.
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GetDependencies returns the logger and metrics handler for creating child retry managers
func (r *RetryManager) GetDependencies() (log.Logger, DSQLMetrics, RetryConfig) {
	return r.logger, r.metrics, r.config
}

// RunTx executes fn in a transaction with DSQL retry semantics.
// op is an operation label for metrics/logging (e.g. "LockTaskQueues", "UpdateShards").
func (r *RetryManager) RunTx(ctx context.Context, op string, fn func(*sql.Tx) (interface{}, error)) (interface{}, error) {
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.ObserveTxLatency(op, time.Since(start))
		}
	}()

	var lastErr error
	maxAttempts := r.config.MaxRetries + 1 // attempts = initial try + retries

	// Reusable timer to reduce allocations.
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		// Respect cancellation early.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}

		result, fnErr := fn(tx)
		if fnErr != nil {
			lastErr = fnErr

			// Best-effort rollback.
			if rbErr := tx.Rollback(); rbErr != nil && r.logger != nil {
				r.logger.Warn("dsql: rollback failed after fn error", 
					tag.NewStringTag("operation", op), 
					tag.NewInt("attempt", attempt), 
					tag.Error(rbErr))
			}

			class := ClassifyError(fnErr)
			if r.metrics != nil {
				r.metrics.IncTxErrorClass(op, class.String())
			}
			
			// Log the error with appropriate severity
			LogError(r.logger, fnErr, op, attempt)

			// Never retry CAS/condition failures.
			if class == ErrorTypeConditionFailed {
				return nil, fnErr
			}

			// Retry only SQLSTATE 40001.
			if class != ErrorTypeRetryable {
				return nil, fnErr
			}

			// Retry budget exhausted.
			if attempt == maxAttempts {
				if r.metrics != nil {
					r.metrics.IncTxExhausted(op)
				}
				return nil, fmt.Errorf("dsql: max retries exceeded (op=%s, attempts=%d): %w", op, attempt, lastErr)
			}

			// Backoff then retry.
			if r.metrics != nil {
				r.metrics.IncTxConflict(op)
				r.metrics.IncTxRetry(op, attempt)
			}

			delay := r.calculateBackoff(attempt - 1) // attempt-1 => 0-based exponent
			if r.metrics != nil {
				r.metrics.ObserveTxBackoff(op, delay)
			}

			if err := sleepWithContext(ctx, &timer, delay); err != nil {
				return nil, err
			}
			continue
		}

		commitErr := tx.Commit()
		if commitErr != nil {
			lastErr = commitErr

			// NOTE: rollback after commit failure is not generally required/valid.
			// The driver may already have rolled back or the commit may have failed
			// during finalization; attempting rollback can yield confusing errors.
			class := ClassifyError(commitErr)
			if r.metrics != nil {
				r.metrics.IncTxErrorClass(op, class.String())
			}
			
			// Log the error with appropriate severity
			LogError(r.logger, commitErr, op, attempt)

			if class == ErrorTypeConditionFailed {
				return nil, commitErr
			}

			// Retry only SQLSTATE 40001.
			if class == ErrorTypeRetryable && attempt < maxAttempts {
				if r.metrics != nil {
					r.metrics.IncTxConflict(op)
					r.metrics.IncTxRetry(op, attempt)
				}

				delay := r.calculateBackoff(attempt - 1)
				if r.metrics != nil {
					r.metrics.ObserveTxBackoff(op, delay)
				}

				if err := sleepWithContext(ctx, &timer, delay); err != nil {
					return nil, err
				}
				continue
			}

			if class == ErrorTypeRetryable && attempt == maxAttempts {
				if r.metrics != nil {
					r.metrics.IncTxExhausted(op)
				}
				return nil, fmt.Errorf("dsql: max retries exceeded (op=%s, attempts=%d): %w", op, attempt, lastErr)
			}

			// Unsupported feature or permanent error.
			return nil, commitErr
		}

		return result, nil
	}

	// Should be unreachable.
	return nil, fmt.Errorf("dsql: unexpected retry loop exit (op=%s): %w", op, lastErr)
}

// RunTxVoid executes fn in a transaction with DSQL retry semantics.
func (r *RetryManager) RunTxVoid(ctx context.Context, op string, fn func(*sql.Tx) error) error {
	_, err := r.RunTx(ctx, op, func(tx *sql.Tx) (interface{}, error) {
		return nil, fn(tx)
	})
	return err
}

//
// Classification and helpers
//

// calculateBackoff calculates backoff delay with exponential backoff and jitter.
// attempt is 0-based (0 => BaseDelay, 1 => 2*BaseDelay, ...).
func (r *RetryManager) calculateBackoff(attempt int) time.Duration {
	// base * 2^attempt
	delay := float64(r.config.BaseDelay) * math.Pow(2, float64(attempt))

	// cap
	if delay > float64(r.config.MaxDelay) {
		delay = float64(r.config.MaxDelay)
	}

	// jitter in [-jitterFactor, +jitterFactor]
	if r.config.JitterFactor > 0 && r.rng != nil {
		j := (r.rng.Float64()*2 - 1) * r.config.JitterFactor
		delay = delay + (delay * j)
	}

	// ensure non-negative
	if delay < 0 {
		delay = float64(r.config.BaseDelay)
	}

	return time.Duration(delay)
}

func sleepWithContext(ctx context.Context, timer **time.Timer, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	if *timer == nil {
		*timer = time.NewTimer(d)
	} else {
		(*timer).Reset(d)
	}

	select {
	case <-(*timer).C:
		return nil
	case <-ctx.Done():
		// Stop timer to avoid leaks; drain if needed.
		if !(*timer).Stop() {
			select {
			case <-(*timer).C:
			default:
			}
		}
		return ctx.Err()
	}
}

//
// Backwards-compatible helpers (optional)
//

// RunTxWithRetry executes fn in a transaction using default config and no metrics/logging.
// Prefer using RetryManager.RunTx so configured retries/metrics are applied.
func RunTxWithRetry(ctx context.Context, db *sql.DB, fn func(*sql.Tx) (interface{}, error)) (interface{}, error) {
	rm := &RetryManager{
		db:     db,
		config: DefaultRetryConfig().validate(),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return rm.RunTx(ctx, "unknown", fn)
}

// RunTxWithRetryVoid executes fn in a transaction using default config and no metrics/logging.
// Prefer using RetryManager.RunTxVoid so configured retries/metrics are applied.
func RunTxWithRetryVoid(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	rm := &RetryManager{
		db:     db,
		config: DefaultRetryConfig().validate(),
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	return rm.RunTxVoid(ctx, "unknown", fn)
}