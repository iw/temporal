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
type dsqlTxRetryPolicy struct {
	maxRetries   int
	baseDelay    time.Duration
	maxDelay     time.Duration
	jitterFactor float64
	rng          *rand.Rand
}

var _ persistsql.TxRetryPolicy = (*dsqlTxRetryPolicy)(nil)

func NewDSQLTxRetryPolicy(rm *RetryManager) persistsql.TxRetryPolicy {
	cfg := rm.config.validate()
	return &dsqlTxRetryPolicy{
		maxRetries:   cfg.MaxRetries,
		baseDelay:    cfg.BaseDelay,
		maxDelay:     cfg.MaxDelay,
		jitterFactor: cfg.JitterFactor,
		rng:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
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
