package dsql

import (
	"context"
	"database/sql"
)

// ExecuteInTxWithRetry is a convenience for wrapping a multi-statement transaction body
// in RetryManager.RunTxVoid. This is intended to be used at the Temporal persistence
// transaction boundary (BeginTx -> body -> Commit) so that commit-time 40001 conflicts
// can be retried safely by replaying the entire body.
func ExecuteInTxWithRetry(ctx context.Context, rm *RetryManager, op string, fn func(tx *sql.Tx) error) error {
	return rm.RunTxVoid(ctx, op, fn)
}
