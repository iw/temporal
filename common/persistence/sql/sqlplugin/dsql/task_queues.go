package dsql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	taskQueueCreatePart = `INTO task_queues_v2 (range_hash, task_queue_id, range_id, data, data_encoding) ` +
		`VALUES (:range_hash, :task_queue_id, :range_id, :data, :data_encoding)`

	// (default range ID: initialRangeID == 1)
	createTaskQueueQry = `INSERT ` + taskQueueCreatePart

	// NOTE: updateTaskQueueQry is intentionally unfenced (matches upstream Postgres plugin semantics).
	// On DSQL, callers should prefer UpdateTaskQueuesWithFencing / UpdateTaskQueuesAutoFenced for correctness.
	updateTaskQueueQry = `UPDATE task_queues_v2 SET
	range_id = :range_id,
	data = :data,
	data_encoding = :data_encoding
	WHERE
	range_hash = :range_hash AND
	task_queue_id = :task_queue_id
	`

	listTaskQueueRowSelect = `SELECT range_hash, task_queue_id, range_id, data, data_encoding FROM task_queues_v2 `

	listTaskQueueWithHashRangeQry = listTaskQueueRowSelect +
		`WHERE range_hash >= $1 AND range_hash <= $2 AND task_queue_id > $3 ORDER BY task_queue_id ASC LIMIT $4`

	listTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = $1 AND task_queue_id > $2 ORDER BY task_queue_id ASC LIMIT $3`

	getTaskQueueQry = listTaskQueueRowSelect +
		`WHERE range_hash = $1 AND task_queue_id=$2`

	deleteTaskQueueQry = `DELETE FROM task_queues_v2 WHERE range_hash=$1 AND task_queue_id=$2 AND range_id=$3`

	lockTaskQueueQry = `SELECT range_id FROM task_queues_v2 ` +
		`WHERE range_hash=$1 AND task_queue_id=$2 FOR UPDATE`
)

// InsertIntoTaskQueues inserts one or more rows into task_queues[_v2] table
func (pdb *db) InsertIntoTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(createTaskQueueQry, v),
		row,
	)
}

// UpdateTaskQueues updates a row in task_queues[_v2] table.
// DSQL Override: Replace unsafe unfenced UPDATE with an auto-fenced CAS update.
// This preserves the sqlplugin interface while preventing lost updates under DSQL OCC.
//
// The auto-fenced update reads the current range_id and attempts a conditional update
// against that value. It does NOT assume "newRangeID-1".
func (pdb *db) UpdateTaskQueues(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return pdb.UpdateTaskQueuesAutoFenced(ctx, row, v)
}

// UpdateTaskQueuesAutoFenced performs a conditional update without relying on any inferred
// expected range_id (e.g. "newRangeID-1").
//
// It reads the current range_id and attempts to update only if the row is unchanged.
// If the new range_id is not greater than the current range_id, it returns ConditionFailedError.
func (pdb *db) UpdateTaskQueuesAutoFenced(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	var currentRangeID int64
	var err error

	lockQry := sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v)

	// We intentionally use lockTaskQueueQry (FOR UPDATE). In DSQL this does not block, but it is
	// a supported primitive and matches the intention of reading a stable fencing token.
	if pdb.tx != nil {
		err = pdb.tx.QueryRowContext(ctx, lockQry, row.RangeHash, row.TaskQueueID).Scan(&currentRangeID)
	} else {
		err = pdb.QueryRowContext(ctx, lockQry, row.RangeHash, row.TaskQueueID).Scan(&currentRangeID)
	}
	if err != nil {
		return nil, err
	}

	// Enforce monotonicity: range_id must increase.
	if row.RangeID <= currentRangeID {
		return nil, NewConditionFailedError(
			ConditionFailedTaskQueue,
			fmt.Sprintf("task queue %s range_id not increasing (current=%d new=%d)", row.TaskQueueID, currentRangeID, row.RangeID),
		)
	}

	// Attempt fenced update against the observed current value.
	return pdb.UpdateTaskQueuesWithFencing(ctx, row, currentRangeID, v)
}

// SelectFromTaskQueues reads one or more rows from task_queues[_v2] table
func (pdb *db) SelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	switch {
	case filter.TaskQueueID != nil:
		if filter.RangeHashLessThanEqualTo != 0 || filter.RangeHashGreaterThanEqualTo != 0 {
			return nil, serviceerror.NewInternal("shardID range not supported for specific selection")
		}
		return pdb.selectFromTaskQueues(ctx, filter, v)
	case filter.RangeHashLessThanEqualTo != 0 && filter.PageSize != nil:
		if filter.RangeHashLessThanEqualTo < filter.RangeHashGreaterThanEqualTo {
			return nil, serviceerror.NewInternal("range of hashes bound is invalid")
		}
		return pdb.rangeSelectFromTaskQueues(ctx, filter, v)
	case filter.TaskQueueIDGreaterThan != nil && filter.PageSize != nil:
		return pdb.rangeSelectFromTaskQueues(ctx, filter, v)
	default:
		return nil, serviceerror.NewInternal("invalid set of query filter params")
	}
}

func (pdb *db) selectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var row sqlplugin.TaskQueuesRow
	err = pdb.GetContext(ctx,
		&row,
		sqlplugin.SwitchTaskQueuesTable(getTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
	)
	if err != nil {
		return nil, err
	}
	return []sqlplugin.TaskQueuesRow{row}, err
}

func (pdb *db) rangeSelectFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) ([]sqlplugin.TaskQueuesRow, error) {
	var err error
	var rows []sqlplugin.TaskQueuesRow
	if filter.RangeHashLessThanEqualTo > 0 {
		err = pdb.SelectContext(ctx,
			&rows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueWithHashRangeQry, v),
			filter.RangeHashGreaterThanEqualTo,
			filter.RangeHashLessThanEqualTo,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	} else {
		err = pdb.SelectContext(ctx,
			&rows,
			sqlplugin.SwitchTaskQueuesTable(listTaskQueueQry, v),
			filter.RangeHash,
			filter.TaskQueueIDGreaterThan,
			*filter.PageSize,
		)
	}
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// DeleteFromTaskQueues deletes a row from task_queues[_v2] table
func (pdb *db) DeleteFromTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(deleteTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
		*filter.RangeID,
	)
}

// LockTaskQueues locks a row in task_queues[_v2] table
// DSQL Override: Wraps FOR UPDATE operation in retry logic for SQLSTATE 40001 handling
// Call-site contract: All subsequent updates must use CAS with range_id fencing
func (pdb *db) LockTaskQueues(
	ctx context.Context,
	filter sqlplugin.TaskQueuesFilter,
	v sqlplugin.MatchingTaskVersion,
) (int64, error) {
	// Use retry manager if available (for non-transaction contexts)
	if pdb.tx == nil && pdb.retryManager != nil {
		result, err := pdb.retryManager.RunTx(ctx, "LockTaskQueues", func(tx *sql.Tx) (interface{}, error) {
			var rangeID int64
			query := sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v)
			err := tx.QueryRowContext(ctx, query, filter.RangeHash, filter.TaskQueueID).Scan(&rangeID)
			return rangeID, err
		})
		if err != nil {
			return 0, err
		}
		return result.(int64), nil
	}

	// Direct execution for transaction contexts (retry handled at higher level)
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		sqlplugin.SwitchTaskQueuesTable(lockTaskQueueQry, v),
		filter.RangeHash,
		filter.TaskQueueID,
	)
	return rangeID, err
}

// UpdateTaskQueuesWithCAS performs a conditional update on task_queues table
// using range_id as fencing token.
func (pdb *db) UpdateTaskQueuesWithCAS(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	expectedRangeID int64,
	v sqlplugin.MatchingTaskVersion,
) error {
	const updateTaskQueueWithCASQry = `UPDATE task_queues_v2 SET
		range_id = :range_id,
		data = :data,
		data_encoding = :data_encoding
		WHERE
		range_hash = :range_hash AND
		task_queue_id = :task_queue_id AND
		range_id = :expected_range_id`

	args := map[string]interface{}{
		"range_hash":        row.RangeHash,
		"task_queue_id":     row.TaskQueueID,
		"range_id":          row.RangeID,
		"data":              row.Data,
		"data_encoding":     row.DataEncoding,
		"expected_range_id": expectedRangeID,
	}

	result, err := pdb.NamedExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(updateTaskQueueWithCASQry, v),
		args,
	)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return NewConditionFailedError(
			ConditionFailedTaskQueue,
			fmt.Sprintf("task_queue %s range_id changed from expected %d (CAS update failed)", row.TaskQueueID, expectedRangeID),
		)
	}

	return nil
}

// UpdateTaskQueuesWithFencing performs a fenced update on task_queues table using range_id as fencing token
func (pdb *db) UpdateTaskQueuesWithFencing(
	ctx context.Context,
	row *sqlplugin.TaskQueuesRow,
	expectedRangeID int64,
	v sqlplugin.MatchingTaskVersion,
) (sql.Result, error) {
	err := pdb.UpdateTaskQueuesWithCAS(ctx, row, expectedRangeID, v)
	if err != nil {
		return nil, err
	}

	// Return a dummy result since UpdateTaskQueuesWithCAS doesn't return sql.Result
	// This maintains interface compatibility
	return &dummyResult{rowsAffected: 1}, nil
}

// UpdateTaskQueueRangeWithCAS performs a conditional range_id increment on task_queues table.
func (pdb *db) UpdateTaskQueueRangeWithCAS(
	ctx context.Context,
	rangeHash uint32,
	taskQueueID string,
	expectedRangeID int64,
	v sqlplugin.MatchingTaskVersion,
) (int64, error) {
	newRangeID := expectedRangeID + 1
	const updateTaskQueueRangeWithCASQry = `UPDATE task_queues_v2 
		SET range_id = $1 
		WHERE range_hash = $2 AND task_queue_id = $3 AND range_id = $4`

	result, err := pdb.ExecContext(ctx,
		sqlplugin.SwitchTaskQueuesTable(updateTaskQueueRangeWithCASQry, v),
		newRangeID,
		rangeHash,
		taskQueueID,
		expectedRangeID,
	)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if rowsAffected == 0 {
		return 0, NewConditionFailedError(
			ConditionFailedTaskQueue,
			fmt.Sprintf("task_queue %s range_id changed from expected %d (CAS range increment failed)", taskQueueID, expectedRangeID),
		)
	}

	return newRangeID, nil
}