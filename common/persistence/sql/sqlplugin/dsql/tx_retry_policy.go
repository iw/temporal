package dsql

import (
	"errors"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	persistsql "go.temporal.io/server/common/persistence/sql"
)

// dsqlTxRetryPolicy enables SqlStore txExecute retry for DSQL only.
// It also records DSQL-specific metrics for transaction retries.
type dsqlTxRetryPolicy struct {
	maxRetries   int
	baseDelay    time.Duration
	maxDelay     time.Duration
	jitterFactor float64
	rng          *rand.Rand
	metrics      DSQLMetrics
}

var _ persistsql.TxRetryPolicy = (*dsqlTxRetryPolicy)(nil)
var _ persistsql.TxRetryMetrics = (*dsqlTxRetryPolicy)(nil)

func NewDSQLTxRetryPolicy(rm *RetryManager) persistsql.TxRetryPolicy {
	cfg := rm.config.validate()
	return &dsqlTxRetryPolicy{
		maxRetries:   cfg.MaxRetries,
		baseDelay:    cfg.BaseDelay,
		maxDelay:     cfg.MaxDelay,
		jitterFactor: cfg.JitterFactor,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
		metrics:      rm.metrics,
	}
}

// TxRetryMetrics implementation

// OnRetry is called when a retry attempt is about to be made
func (p *dsqlTxRetryPolicy) OnRetry(operation string, attempt int, err error, backoff time.Duration) {
	if p.metrics == nil {
		return
	}
	p.metrics.IncTxConflict(operation)
	p.metrics.IncTxRetry(operation, attempt)
	p.metrics.ObserveTxBackoff(operation, backoff)

	// Also record error class
	cls := classifyError(err)
	p.metrics.IncTxErrorClass(operation, cls.String())
}

// OnExhausted is called when all retry attempts have been exhausted
func (p *dsqlTxRetryPolicy) OnExhausted(operation string, attempts int, err error) {
	if p.metrics == nil {
		return
	}
	p.metrics.IncTxExhausted(operation)
}

// OnSuccess is called when the operation succeeds
func (p *dsqlTxRetryPolicy) OnSuccess(operation string, attempts int) {
	// Currently we don't record success metrics, but this hook is available
	// for future use (e.g., recording latency histograms by attempt count)
}

func (p *dsqlTxRetryPolicy) MaxRetries() int {
	return p.maxRetries
}

func (p *dsqlTxRetryPolicy) Backoff(attempt int) time.Duration {
	delay := float64(p.baseDelay) * math.Pow(2, float64(attempt))
	if delay > float64(p.maxDelay) {
		delay = float64(p.maxDelay)
	}
	if p.jitterFactor > 0 && p.rng != nil {
		j := (p.rng.Float64()*2 - 1) * p.jitterFactor
		delay = delay + (delay * j)
	}
	if delay < 0 {
		delay = 0
	}
	return time.Duration(delay)
}

// RetryableTxError returns true only for retryable OCC conflicts.
// ConditionFailedError must never be retried.
func (p *dsqlTxRetryPolicy) RetryableTxError(err error) bool {
	if IsConditionFailedError(err) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "40001"
	}
	return false
}
