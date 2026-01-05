// Package dsql provides Aurora DSQL-specific conditional update (CAS) helpers.
//
// This file implements Compare-And-Swap (CAS) update patterns that replace
// traditional locking mechanisms with optimistic concurrency control suitable
// for Aurora DSQL's architecture.
//
// Key principles:
//   1) All updates use conditional WHERE clauses with fencing tokens
//   2) rowsAffected == 0 indicates condition failure (ownership lost)
//   3) ConditionFailedError is returned for CAS failures (not retried)
//   4) SQLSTATE 40001 conflicts are handled by retry framework
package dsql

import (
	"context"
	"database/sql"
	"fmt"
)

// UpdateShardWithCAS performs a conditional update on the shards table using range_id as fencing token.
// This replaces traditional FOR UPDATE locking with optimistic concurrency control.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - shardID: The shard to update
//   - newRangeID: The new range_id value to set
//   - expectedRangeID: The expected current range_id (fencing token)
//   - data: The shard data to update
//   - dataEncoding: The encoding type for the data
//
// Returns:
//   - ConditionFailedError if expectedRangeID doesn't match current value
//   - Other errors for database failures
//
// Usage pattern:
//   1. Read current range_id via ReadLockShards
//   2. Call UpdateShardWithCAS with current range_id as expectedRangeID
//   3. Handle ConditionFailedError as ownership lost (normal under contention)
func (pdb *db) UpdateShardWithCAS(
	ctx context.Context,
	shardID int32,
	newRangeID int64,
	expectedRangeID int64,
	data []byte,
	dataEncoding string,
) error {
	const updateShardWithCASQry = `UPDATE shards 
		SET range_id = $1, data = $2, data_encoding = $3 
		WHERE shard_id = $4 AND range_id = $5`

	result, err := pdb.ExecContext(ctx,
		updateShardWithCASQry,
		newRangeID,
		data,
		dataEncoding,
		shardID,
		expectedRangeID,
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
			"shard %d range_id changed from expected %d (CAS update failed)",
			shardID, expectedRangeID,
		)
	}

	return nil
}

// UpdateShardRangeWithCAS performs a conditional range_id increment on the shards table.
// This is a specialized CAS operation for shard ownership transfers.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - shardID: The shard to update
//   - expectedRangeID: The expected current range_id (fencing token)
//
// Returns:
//   - The new range_id value (expectedRangeID + 1) on success
//   - ConditionFailedError if expectedRangeID doesn't match current value
//   - Other errors for database failures
func (pdb *db) UpdateShardRangeWithCAS(
	ctx context.Context,
	shardID int32,
	expectedRangeID int64,
) (int64, error) {
	newRangeID := expectedRangeID + 1
	const updateShardRangeWithCASQry = `UPDATE shards 
		SET range_id = $1 
		WHERE shard_id = $2 AND range_id = $3`

	result, err := pdb.ExecContext(ctx,
		updateShardRangeWithCASQry,
		newRangeID,
		shardID,
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
			"shard %d range_id changed from expected %d (CAS range increment failed)",
			shardID, expectedRangeID,
		)
	}

	return newRangeID, nil
}

// GenericCASUpdate provides a generic conditional update utility for any table.
// This helper encapsulates the common CAS pattern used across different tables.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - query: The UPDATE query with conditional WHERE clause
//   - args: Query arguments (must include fencing token in WHERE clause)
//   - entityDesc: Description of the entity being updated (for error messages)
//   - fencingToken: The expected fencing token value (for error messages)
//
// Returns:
//   - ConditionFailedError if the conditional update affects 0 rows
//   - Other errors for database failures
//
// Example usage:
//   err := pdb.GenericCASUpdate(ctx,
//       "UPDATE task_queues_v2 SET range_id = $1 WHERE range_hash = $2 AND task_queue_id = $3 AND range_id = $4",
//       []interface{}{newRangeID, rangeHash, taskQueueID, expectedRangeID},
//       fmt.Sprintf("task_queue %s", taskQueueID),
//       expectedRangeID,
//   )
func (pdb *db) GenericCASUpdate(
	ctx context.Context,
	query string,
	args []interface{},
	entityDesc string,
	fencingToken interface{},
) error {
	result, err := pdb.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return NewConditionFailedError(
			"%s fencing token changed from expected %v (CAS update failed)",
			entityDesc, fencingToken,
		)
	}

	return nil
}

// ValidateRowsAffected is a helper function to validate that exactly one row was affected
// by an update operation. This is commonly used in CAS operations to ensure atomicity.
//
// Parameters:
//   - result: The sql.Result from an UPDATE operation
//   - entityDesc: Description of the entity being updated (for error messages)
//   - fencingToken: The expected fencing token value (for error messages)
//
// Returns:
//   - ConditionFailedError if rowsAffected == 0 (condition failed)
//   - Error if rowsAffected > 1 (unexpected, indicates data corruption)
//   - nil if rowsAffected == 1 (success)
func ValidateRowsAffected(result sql.Result, entityDesc string, fencingToken interface{}) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	switch rowsAffected {
	case 0:
		return NewConditionFailedError(
			"%s fencing token changed from expected %v (CAS validation failed)",
			entityDesc, fencingToken,
		)
	case 1:
		return nil
	default:
		return fmt.Errorf(
			"unexpected rows affected: %d (expected 1) for %s with fencing token %v",
			rowsAffected, entityDesc, fencingToken,
		)
	}
}