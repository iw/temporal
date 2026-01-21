package dsql

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"time"

	"go.temporal.io/server/common/metrics"
)

// DSQLMetrics defines the interface for DSQL-specific metrics.
// This interface is used by RetryManager to record metrics during retry operations.
type DSQLMetrics interface {
	// ObserveTxLatency records the total latency of a transaction operation
	ObserveTxLatency(op string, latency time.Duration)

	// IncTxErrorClass increments the counter for a specific error class
	IncTxErrorClass(op string, errorClass string)

	// IncTxExhausted increments the counter when retry budget is exhausted
	IncTxExhausted(op string)

	// IncTxConflict increments the counter for transaction conflicts (SQLSTATE 40001)
	IncTxConflict(op string)

	// IncTxRetry increments the counter for retry attempts
	IncTxRetry(op string, attempt int)

	// ObserveTxBackoff records the backoff delay duration
	ObserveTxBackoff(op string, delay time.Duration)

	// StartPoolCollector starts a background goroutine that periodically samples
	// connection pool statistics and records them as metrics.
	StartPoolCollector(db *sql.DB, interval time.Duration)

	// StopPoolCollector stops the background pool metrics collector.
	StopPoolCollector()

	// GetHandler returns the underlying metrics handler, or nil if not available
	GetHandler() metrics.Handler
}

// dsqlMetricsImpl implements the DSQLMetrics interface using Temporal's metrics system
type dsqlMetricsImpl struct {
	handler      metrics.Handler
	txLatency    metrics.TimerIface
	txErrorClass metrics.CounterIface
	txExhausted  metrics.CounterIface
	txConflict   metrics.CounterIface
	txRetry      metrics.CounterIface
	txBackoff    metrics.TimerIface

	// Connection pool metrics
	poolMaxOpen      metrics.GaugeIface
	poolOpen         metrics.GaugeIface
	poolInUse        metrics.GaugeIface
	poolIdle         metrics.GaugeIface
	poolWaitCount    metrics.CounterIface
	poolWaitDuration metrics.TimerIface

	// Connection closure metrics (cumulative counters from db.Stats())
	poolClosedMaxLifetime metrics.GaugeIface
	poolClosedMaxIdleTime metrics.GaugeIface
	poolClosedMaxIdle     metrics.GaugeIface

	// Legacy metrics for backward compatibility
	retryCounter              metrics.CounterIface
	conflictCounter           metrics.CounterIface
	conditionFailedCounter    metrics.CounterIface
	operationDuration         metrics.TimerIface
	retryAttempts             metrics.HistogramIface
	activeTransactions        metrics.GaugeIface
	errorCounter              metrics.CounterIface
	unsupportedFeatureCounter metrics.CounterIface

	// Pool collector state
	poolCollectorMu     sync.Mutex
	poolCollectorCancel context.CancelFunc
	lastWaitCount       int64
}

// NewDSQLMetrics creates a new DSQLMetrics implementation.
// If metricsHandler is nil, returns a no-op implementation.
func NewDSQLMetrics(metricsHandler metrics.Handler) DSQLMetrics {
	if metricsHandler == nil {
		return &noOpDSQLMetrics{}
	}

	return &dsqlMetricsImpl{
		handler:      metricsHandler,
		txLatency:    metricsHandler.Timer("dsql_tx_latency"),
		txErrorClass: metricsHandler.Counter("dsql_tx_error_class_total"),
		txExhausted:  metricsHandler.Counter("dsql_tx_exhausted_total"),
		txConflict:   metricsHandler.Counter("dsql_tx_conflict_total"),
		txRetry:      metricsHandler.Counter("dsql_tx_retry_total"),
		txBackoff:    metricsHandler.Timer("dsql_tx_backoff"),

		// Connection pool metrics
		poolMaxOpen:      metricsHandler.Gauge("dsql_pool_max_open"),
		poolOpen:         metricsHandler.Gauge("dsql_pool_open"),
		poolInUse:        metricsHandler.Gauge("dsql_pool_in_use"),
		poolIdle:         metricsHandler.Gauge("dsql_pool_idle"),
		poolWaitCount:    metricsHandler.Counter("dsql_pool_wait_total"),
		poolWaitDuration: metricsHandler.Timer("dsql_pool_wait_duration"),

		// Connection closure metrics - cumulative counters showing WHY connections closed
		poolClosedMaxLifetime: metricsHandler.Gauge("dsql_db_closed_max_lifetime_total"),
		poolClosedMaxIdleTime: metricsHandler.Gauge("dsql_db_closed_max_idle_time_total"),
		poolClosedMaxIdle:     metricsHandler.Gauge("dsql_db_closed_max_idle_total"),

		// Legacy metrics for backward compatibility
		retryCounter:              metricsHandler.Counter("dsql_tx_retries_total"),
		conflictCounter:           metricsHandler.Counter("dsql_tx_conflicts_total"),
		conditionFailedCounter:    metricsHandler.Counter("dsql_condition_failed_total"),
		operationDuration:         metricsHandler.Timer("dsql_operation_duration"),
		retryAttempts:             metricsHandler.Histogram("dsql_retry_attempts", metrics.Milliseconds),
		activeTransactions:        metricsHandler.Gauge("dsql_active_transactions"),
		errorCounter:              metricsHandler.Counter("dsql_errors_total"),
		unsupportedFeatureCounter: metricsHandler.Counter("dsql_unsupported_feature_total"),
	}
}

// GetHandler returns the underlying metrics handler, or nil if this is a no-op implementation
func (m *dsqlMetricsImpl) GetHandler() metrics.Handler {
	return m.handler
}

// GetHandler returns nil for no-op implementation
func (n *noOpDSQLMetrics) GetHandler() metrics.Handler {
	return nil
}

// ObserveTxLatency records the total latency of a transaction operation
func (m *dsqlMetricsImpl) ObserveTxLatency(op string, latency time.Duration) {
	m.txLatency.Record(latency, metrics.StringTag("operation", op))
}

// IncTxErrorClass increments the counter for a specific error class
func (m *dsqlMetricsImpl) IncTxErrorClass(op string, errorClass string) {
	m.txErrorClass.Record(1,
		metrics.StringTag("operation", op),
		metrics.StringTag("error_class", errorClass),
	)
}

// IncTxExhausted increments the counter when retry budget is exhausted
func (m *dsqlMetricsImpl) IncTxExhausted(op string) {
	m.txExhausted.Record(1, metrics.StringTag("operation", op))
}

// IncTxConflict increments the counter for transaction conflicts (SQLSTATE 40001)
func (m *dsqlMetricsImpl) IncTxConflict(op string) {
	m.txConflict.Record(1, metrics.StringTag("operation", op))
}

// IncTxRetry increments the counter for retry attempts
func (m *dsqlMetricsImpl) IncTxRetry(op string, attempt int) {
	m.txRetry.Record(1,
		metrics.StringTag("operation", op),
		metrics.StringTag("attempt", strconv.Itoa(attempt)),
	)
}

// ObserveTxBackoff records the backoff delay duration
func (m *dsqlMetricsImpl) ObserveTxBackoff(op string, delay time.Duration) {
	m.txBackoff.Record(delay, metrics.StringTag("operation", op))
}

// StartPoolCollector starts a background goroutine that periodically samples
// connection pool statistics and records them as metrics.
// Call StopPoolCollector to stop the collector when the DB is closed.
func (m *dsqlMetricsImpl) StartPoolCollector(db *sql.DB, interval time.Duration) {
	m.poolCollectorMu.Lock()
	defer m.poolCollectorMu.Unlock()

	// Don't start if already running
	if m.poolCollectorCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.poolCollectorCancel = cancel

	go m.runPoolCollector(ctx, db, interval)
}

// StopPoolCollector stops the background pool metrics collector.
func (m *dsqlMetricsImpl) StopPoolCollector() {
	m.poolCollectorMu.Lock()
	defer m.poolCollectorMu.Unlock()

	if m.poolCollectorCancel != nil {
		m.poolCollectorCancel()
		m.poolCollectorCancel = nil
	}
}

// runPoolCollector is the background goroutine that samples pool stats.
func (m *dsqlMetricsImpl) runPoolCollector(ctx context.Context, db *sql.DB, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.recordPoolStats(db)
		}
	}
}

// recordPoolStats samples the current pool statistics and records them.
func (m *dsqlMetricsImpl) recordPoolStats(db *sql.DB) {
	stats := db.Stats()

	// Record gauge metrics for current pool state
	m.poolMaxOpen.Record(float64(stats.MaxOpenConnections))
	m.poolOpen.Record(float64(stats.OpenConnections))
	m.poolInUse.Record(float64(stats.InUse))
	m.poolIdle.Record(float64(stats.Idle))

	// Record wait count as a counter (delta from last sample)
	// WaitCount is cumulative, so we track the delta
	if stats.WaitCount > m.lastWaitCount {
		delta := stats.WaitCount - m.lastWaitCount
		m.poolWaitCount.Record(delta)
	}
	m.lastWaitCount = stats.WaitCount

	// Record wait duration (cumulative, but we record the current total)
	if stats.WaitDuration > 0 {
		m.poolWaitDuration.Record(stats.WaitDuration)
	}

	// Record connection closure counters (cumulative totals)
	// These tell us WHY connections are being closed:
	// - MaxLifetimeClosed: closed due to MaxConnLifetime (expected after 55 min)
	// - MaxIdleTimeClosed: closed due to MaxConnIdleTime (should be 0 if set to 0)
	// - MaxIdleClosed: closed because idle pool was full (shouldn't happen if MaxIdleConns = MaxConns)
	m.poolClosedMaxLifetime.Record(float64(stats.MaxLifetimeClosed))
	m.poolClosedMaxIdleTime.Record(float64(stats.MaxIdleTimeClosed))
	m.poolClosedMaxIdle.Record(float64(stats.MaxIdleClosed))
}

// Legacy methods for backward compatibility

// RecordRetry records a retry attempt (legacy method)
func (m *dsqlMetricsImpl) RecordRetry(operation, sqlstate string, attempt int) {
	m.retryCounter.Record(1,
		metrics.StringTag("operation", operation),
		metrics.StringTag("sqlstate", sqlstate),
		metrics.StringTag("attempt", strconv.Itoa(attempt)),
	)
}

// RecordConflict records a DSQL serialization conflict (legacy method)
func (m *dsqlMetricsImpl) RecordConflict(operation string) {
	m.conflictCounter.Record(1,
		metrics.StringTag("operation", operation),
	)
}

// RecordConditionFailed records a CAS condition failure (legacy method)
func (m *dsqlMetricsImpl) RecordConditionFailed(operation string) {
	m.conditionFailedCounter.Record(1,
		metrics.StringTag("operation", operation),
	)
}

// RecordOperationDuration records the duration of a DSQL operation (legacy method)
func (m *dsqlMetricsImpl) RecordOperationDuration(operation string, duration time.Duration) {
	m.operationDuration.Record(duration,
		metrics.StringTag("operation", operation),
	)
}

// RecordRetryAttempts records the number of retry attempts for an operation (legacy method)
func (m *dsqlMetricsImpl) RecordRetryAttempts(operation string, attempts int) {
	m.retryAttempts.Record(int64(attempts),
		metrics.StringTag("operation", operation),
	)
}

// RecordActiveTransaction records the current number of active transactions (legacy method)
func (m *dsqlMetricsImpl) RecordActiveTransaction(operation string, delta float64) {
	m.activeTransactions.Record(delta,
		metrics.StringTag("operation", operation),
	)
}

// RecordError records a DSQL error by type (legacy method)
func (m *dsqlMetricsImpl) RecordError(operation string, errorType ErrorType) {
	m.errorCounter.Record(1,
		metrics.StringTag("operation", operation),
		metrics.StringTag("error_type", errorType.String()),
	)
}

// RecordUnsupportedFeature records when an unsupported feature is encountered (legacy method)
func (m *dsqlMetricsImpl) RecordUnsupportedFeature(operation, feature string) {
	m.unsupportedFeatureCounter.Record(1,
		metrics.StringTag("operation", operation),
		metrics.StringTag("feature", feature),
	)
}

// noOpDSQLMetrics is a no-op implementation of DSQLMetrics for when metrics are disabled
type noOpDSQLMetrics struct{}

func (n *noOpDSQLMetrics) ObserveTxLatency(op string, latency time.Duration)     {}
func (n *noOpDSQLMetrics) IncTxErrorClass(op string, errorClass string)          {}
func (n *noOpDSQLMetrics) IncTxExhausted(op string)                              {}
func (n *noOpDSQLMetrics) IncTxConflict(op string)                               {}
func (n *noOpDSQLMetrics) IncTxRetry(op string, attempt int)                     {}
func (n *noOpDSQLMetrics) ObserveTxBackoff(op string, delay time.Duration)       {}
func (n *noOpDSQLMetrics) StartPoolCollector(db *sql.DB, interval time.Duration) {}
func (n *noOpDSQLMetrics) StopPoolCollector()                                    {}

// DSQLOperationMetrics provides operation-specific metrics recording (legacy)
type DSQLOperationMetrics struct {
	metrics   *dsqlMetricsImpl
	operation string
	startTime time.Time
}

// NewDSQLOperationMetrics creates a new operation-specific metrics recorder (legacy)
func NewDSQLOperationMetrics(metrics DSQLMetrics, operation string) *DSQLOperationMetrics {
	impl, ok := metrics.(*dsqlMetricsImpl)
	if !ok {
		// If metrics is not the expected type, create a dummy one
		impl = &dsqlMetricsImpl{}
	}

	return &DSQLOperationMetrics{
		metrics:   impl,
		operation: operation,
		startTime: time.Now(),
	}
}

// RecordStart records the start of an operation (legacy)
func (m *DSQLOperationMetrics) RecordStart() {
	m.metrics.RecordActiveTransaction(m.operation, 1)
}

// RecordEnd records the end of an operation (legacy)
func (m *DSQLOperationMetrics) RecordEnd() {
	duration := time.Since(m.startTime)
	m.metrics.RecordOperationDuration(m.operation, duration)
	m.metrics.RecordActiveTransaction(m.operation, -1)
}

// RecordRetry records a retry for this operation (legacy)
func (m *DSQLOperationMetrics) RecordRetry(sqlstate string, attempt int) {
	m.metrics.RecordRetry(m.operation, sqlstate, attempt)
}

// RecordConflict records a conflict for this operation (legacy)
func (m *DSQLOperationMetrics) RecordConflict() {
	m.metrics.RecordConflict(m.operation)
}

// RecordConditionFailed records a condition failure for this operation (legacy)
func (m *DSQLOperationMetrics) RecordConditionFailed() {
	m.metrics.RecordConditionFailed(m.operation)
}

// RecordError records an error for this operation (legacy)
func (m *DSQLOperationMetrics) RecordError(errorType ErrorType) {
	m.metrics.RecordError(m.operation, errorType)
}

// RecordUnsupportedFeature records an unsupported feature for this operation (legacy)
func (m *DSQLOperationMetrics) RecordUnsupportedFeature(feature string) {
	m.metrics.RecordUnsupportedFeature(m.operation, feature)
}

// RecordFinalAttempts records the total number of attempts made (legacy)
func (m *DSQLOperationMetrics) RecordFinalAttempts(attempts int) {
	m.metrics.RecordRetryAttempts(m.operation, attempts)
}
