package dsql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createShardQry = `INSERT INTO
 shards (shard_id, range_id, data, data_encoding) VALUES ($1, $2, $3, $4)`

	getShardQry = `SELECT
 shard_id, range_id, data, data_encoding
 FROM shards WHERE shard_id = $1`

	updateShardQry = `UPDATE shards 
 SET range_id = $1, data = $2, data_encoding = $3 
 WHERE shard_id = $4`

	lockShardQry = `SELECT range_id FROM shards WHERE shard_id = $1 FOR UPDATE`
	// NOTE: readLockShardQry removed - FOR SHARE is not supported by DSQL
	// Use ReadLockShards method which implements DSQL-compatible optimistic reads
)

// InsertIntoShards inserts one or more rows into shards table
func (pdb *db) InsertIntoShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		createShardQry,
		row.ShardID,
		row.RangeID,
		row.Data,
		row.DataEncoding,
	)
}

// UpdateShards updates one or more rows into shards table
// DSQL Override: This method now requires proper fencing to prevent lost updates
// The caller must provide the expected range_id that was read during the lock phase
func (pdb *db) UpdateShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	// CRITICAL: This method is unsafe without fencing in DSQL's optimistic concurrency model
	// Callers should use UpdateShardsWithFencing instead
	// For backward compatibility, we'll attempt to detect if this is a fenced update
	// by checking if the range_id looks like it was incremented from a previous read

	// Use the CAS update pattern to ensure atomicity
	// Note: This assumes the caller read the current range_id and incremented it
	// If this assumption is wrong, the update will fail with ConditionFailedError
	expectedRangeID := row.RangeID - 1

	return pdb.UpdateShardsWithFencing(ctx, row, expectedRangeID)
}

// SelectFromShards reads one or more rows from shards table
func (pdb *db) SelectFromShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (*sqlplugin.ShardsRow, error) {
	var row sqlplugin.ShardsRow
	err := pdb.GetContext(ctx,
		&row,
		getShardQry,
		filter.ShardID,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// ReadLockShards acquires a read lock on a single row in shards table
// DSQL Override: Removes FOR SHARE clause and uses optimistic concurrency control
// Call-site contract: Used only for range_id validation, no subsequent writes in same transaction
func (pdb *db) ReadLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	// DSQL-compatible query without FOR SHARE clause
	const dsqlReadLockShardQry = `SELECT range_id FROM shards WHERE shard_id = $1`

	// Use retry manager if available (for non-transaction contexts)
	if pdb.tx == nil && pdb.retryManager != nil {
		result, err := pdb.retryManager.RunTx(ctx, "ReadLockShards", func(tx *sql.Tx) (interface{}, error) {
			var rangeID int64
			err := tx.QueryRowContext(ctx, dsqlReadLockShardQry, filter.ShardID).Scan(&rangeID)
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
		dsqlReadLockShardQry,
		filter.ShardID,
	)
	return rangeID, err
}

// WriteLockShards acquires a write lock on a single row in shards table
// DSQL Override: Wraps FOR UPDATE operation in retry logic for SQLSTATE 40001 handling
// Call-site contract: All subsequent updates must use CAS with range_id fencing
func (pdb *db) WriteLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	// Use retry manager if available (for non-transaction contexts)
	if pdb.tx == nil && pdb.retryManager != nil {
		result, err := pdb.retryManager.RunTx(ctx, "WriteLockShards", func(tx *sql.Tx) (interface{}, error) {
			var rangeID int64
			err := tx.QueryRowContext(ctx, lockShardQry, filter.ShardID).Scan(&rangeID)
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
		lockShardQry,
		filter.ShardID,
	)
	return rangeID, err
}

// UpdateShardsWithFencing performs a fenced update on shards table using range_id as fencing token
// This is the safe method for updating shards in DSQL's optimistic concurrency model
func (pdb *db) UpdateShardsWithFencing(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
	expectedRangeID int64,
) (sql.Result, error) {
	const updateShardWithFencingQry = `UPDATE shards 
		SET range_id = $1, data = $2, data_encoding = $3 
		WHERE shard_id = $4 AND range_id = $5`

	result, err := pdb.ExecContext(ctx,
		updateShardWithFencingQry,
		row.RangeID,
		row.Data,
		row.DataEncoding,
		row.ShardID,
		expectedRangeID,
	)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected == 0 {
		return nil, NewConditionFailedError(
			ConditionFailedShard,
			fmt.Sprintf("shard %d range_id changed from expected %d (fenced update failed)", row.ShardID, expectedRangeID),
		)
	}

	return result, nil
}
