package dsql

import (
	"context"
	"database/sql"
	"fmt"
)

// ExecFencedUpdate executes a conditional UPDATE/DELETE and converts a 0-row result
// into a ConditionFailedError (non-retryable).
func ExecFencedUpdate(
	ctx context.Context,
	tx *sql.Tx,
	kind ConditionFailedKind,
	msg string,
	query string,
	args ...any,
) error {
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return NewConditionFailedError(kind, msg)
	}
	if n != 1 {
		// defensive; should not happen for single-row fencing
		return fmt.Errorf("unexpected rows affected (%d) for fenced update: %s", n, msg)
	}
	return nil
}
