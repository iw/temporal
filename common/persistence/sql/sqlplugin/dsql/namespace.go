package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createNamespaceQuery = `INSERT INTO 
 namespaces (partition_id, id, name, is_global, data, data_encoding, notification_version)
 VALUES($1, $2, $3, $4, $5, $6, $7)`

	updateNamespaceQuery = `UPDATE namespaces 
 SET name = $1, data = $2, data_encoding = $3, is_global = $4, notification_version = $5
 WHERE partition_id=54321 AND id = $6`

	getNamespacePart = `SELECT id, name, is_global, data, data_encoding, notification_version FROM namespaces`

	getNamespaceByIDQuery   = getNamespacePart + ` WHERE partition_id=$1 AND id = $2`
	getNamespaceByNameQuery = getNamespacePart + ` WHERE partition_id=$1 AND name = $2`

	listNamespacesQuery      = getNamespacePart + ` WHERE partition_id=$1 ORDER BY id LIMIT $2`
	listNamespacesRangeQuery = getNamespacePart + ` WHERE partition_id=$1 AND id > $2 ORDER BY id LIMIT $3`

	deleteNamespaceByIDQuery   = `DELETE FROM namespaces WHERE partition_id=$1 AND id = $2`
	deleteNamespaceByNameQuery = `DELETE FROM namespaces WHERE partition_id=$1 AND name = $2`

	getNamespaceMetadataQuery    = `SELECT notification_version FROM namespace_metadata WHERE partition_id=$1`
	lockNamespaceMetadataQuery   = `SELECT notification_version FROM namespace_metadata WHERE partition_id=$1 FOR UPDATE`
	updateNamespaceMetadataQuery = `UPDATE namespace_metadata SET notification_version = $1 WHERE notification_version = $2 AND partition_id=$3`
)

const (
	partitionID = 54321
)

var errMissingArgs = errors.New("missing one or more args for API")

// InsertIntoNamespace inserts a single row into namespaces table
func (pdb *db) InsertIntoNamespace(
	ctx context.Context,
	row *sqlplugin.NamespaceRow,
) (sql.Result, error) {
	// Log the row data for debugging
	fmt.Printf("[DSQL-DEBUG] InsertIntoNamespace: ID=%s, Name=%s, DataType=%T, DataLen=%d, DataEncoding=%s, DataHex=%x\n",
		row.ID, row.Name, row.Data, len(row.Data), row.DataEncoding, row.Data)
	
	// Convert UUID to string as otherwise pgx would write the UUID as []byte
	idStr := row.ID.String()
	
	// Use standard query with UUID as string
	query := `INSERT INTO 
 namespaces (partition_id, id, name, is_global, data, data_encoding, notification_version)
 VALUES($1, $2, $3, $4, $5, $6, $7)`
	return pdb.ExecContext(ctx, query, partitionID, idStr, row.Name, row.IsGlobal, row.Data, row.DataEncoding, row.NotificationVersion)
}

// UpdateNamespace updates a single row in namespaces table
// DSQL Override: This method now requires proper fencing to prevent lost updates
// The caller must provide the expected notification_version that was read during the lock phase
func (pdb *db) UpdateNamespace(
	ctx context.Context,
	row *sqlplugin.NamespaceRow,
) (sql.Result, error) {
	// CRITICAL: This method is unsafe without fencing in DSQL's optimistic concurrency model
	// Callers should use UpdateNamespaceWithFencing instead
	// For backward compatibility, we'll attempt to detect if this is a fenced update
	// by checking if the notification_version looks like it was incremented from a previous read
	
	// Use the CAS update pattern to ensure atomicity
	// Note: This assumes the caller read the current notification_version and incremented it
	// If this assumption is wrong, the update will fail with ConditionFailedError
	expectedNotificationVersion := row.NotificationVersion - 1
	
	return pdb.UpdateNamespaceWithFencing(ctx, row, expectedNotificationVersion)
}

// SelectFromNamespace reads one or more rows from namespaces table
func (pdb *db) SelectFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) ([]sqlplugin.NamespaceRow, error) {
	var res []sqlplugin.NamespaceRow
	var err error
	switch {
	case filter.ID != nil || filter.Name != nil:
		if filter.ID != nil && filter.Name != nil {
			return nil, serviceerror.NewInternal("only ID or name filter can be specified for selection")
		}
		res, err = pdb.selectFromNamespace(ctx, filter)
	case filter.PageSize != nil && *filter.PageSize > 0:
		res, err = pdb.selectAllFromNamespace(ctx, filter)
	default:
		return nil, errMissingArgs
	}

	return res, err
}

func (pdb *db) selectFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) ([]sqlplugin.NamespaceRow, error) {
	var err error
	var row sqlplugin.NamespaceRow
	switch {
	case filter.ID != nil:
		err = pdb.GetContext(ctx,
			&row,
			getNamespaceByIDQuery,
			partitionID,
			*filter.ID,
		)
	case filter.Name != nil:
		err = pdb.GetContext(ctx,
			&row,
			getNamespaceByNameQuery,
			partitionID,
			*filter.Name,
		)
	}
	if err != nil {
		return nil, err
	}
	return []sqlplugin.NamespaceRow{row}, nil
}

func (pdb *db) selectAllFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) ([]sqlplugin.NamespaceRow, error) {
	var err error
	var rows []sqlplugin.NamespaceRow
	switch {
	case filter.GreaterThanID != nil:
		err = pdb.SelectContext(ctx,
			&rows,
			listNamespacesRangeQuery,
			partitionID,
			*filter.GreaterThanID,
			*filter.PageSize,
		)
	default:
		err = pdb.SelectContext(ctx,
			&rows,
			listNamespacesQuery,
			partitionID,
			filter.PageSize,
		)
	}
	return rows, err
}

// DeleteFromNamespace deletes a single row in namespaces table
func (pdb *db) DeleteFromNamespace(
	ctx context.Context,
	filter sqlplugin.NamespaceFilter,
) (sql.Result, error) {
	var err error
	var result sql.Result
	switch {
	case filter.ID != nil:
		result, err = pdb.ExecContext(ctx,
			deleteNamespaceByIDQuery,
			partitionID,
			filter.ID,
		)
	default:
		result, err = pdb.ExecContext(ctx,
			deleteNamespaceByNameQuery,
			partitionID,
			filter.Name,
		)
	}
	return result, err
}

// LockNamespaceMetadata acquires a write lock on a single row in namespace_metadata table
// DSQL Override: Wraps FOR UPDATE operation in retry logic for SQLSTATE 40001 handling
// Call-site contract: All subsequent updates must use CAS with notification_version fencing
func (pdb *db) LockNamespaceMetadata(
	ctx context.Context,
) (*sqlplugin.NamespaceMetadataRow, error) {
	// Use retry manager if available (for non-transaction contexts)
	if pdb.tx == nil && pdb.retryManager != nil {
		result, err := pdb.retryManager.RunTx(ctx, "LockNamespaceMetadata", func(tx *sql.Tx) (interface{}, error) {
			var row sqlplugin.NamespaceMetadataRow
			err := tx.QueryRowContext(ctx, lockNamespaceMetadataQuery, partitionID).Scan(&row.NotificationVersion)
			if err != nil {
				return nil, err
			}
			return &row, nil
		})
		if err != nil {
			return nil, err
		}
		return result.(*sqlplugin.NamespaceMetadataRow), nil
	}
	
	// Direct execution for transaction contexts (retry handled at higher level)
	var row sqlplugin.NamespaceMetadataRow
	err := pdb.GetContext(ctx,
		&row.NotificationVersion,
		lockNamespaceMetadataQuery,
		partitionID,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// SelectFromNamespaceMetadata reads a single row in namespace_metadata table
func (pdb *db) SelectFromNamespaceMetadata(
	ctx context.Context,
) (*sqlplugin.NamespaceMetadataRow, error) {
	var row sqlplugin.NamespaceMetadataRow
	err := pdb.GetContext(ctx,
		&row.NotificationVersion,
		getNamespaceMetadataQuery,
		partitionID,
	)
	return &row, err
}

// UpdateNamespaceMetadata updates a single row in namespace_metadata table
func (pdb *db) UpdateNamespaceMetadata(
	ctx context.Context,
	row *sqlplugin.NamespaceMetadataRow,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		updateNamespaceMetadataQuery,
		row.NotificationVersion+1,
		row.NotificationVersion,
		partitionID,
	)
}

// UpdateNamespaceMetadataWithCAS performs a conditional update on namespace_metadata table
// using notification_version as fencing token. This replaces traditional FOR UPDATE locking
// with optimistic concurrency control suitable for DSQL.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - expectedNotificationVersion: The expected current notification_version (fencing token)
//
// Returns:
//   - The new notification_version value (expectedNotificationVersion + 1) on success
//   - ConditionFailedError if expectedNotificationVersion doesn't match current value
//   - Other errors for database failures
//
// Usage pattern:
//   1. Read current notification_version via LockNamespaceMetadata or SelectFromNamespaceMetadata
//   2. Call UpdateNamespaceMetadataWithCAS with current version as expectedNotificationVersion
//   3. Handle ConditionFailedError as ownership lost (normal under contention)
func (pdb *db) UpdateNamespaceMetadataWithCAS(
	ctx context.Context,
	expectedNotificationVersion int64,
) (int64, error) {
	newNotificationVersion := expectedNotificationVersion + 1
	const updateNamespaceMetadataWithCASQry = `UPDATE namespace_metadata 
		SET notification_version = $1 
		WHERE notification_version = $2 AND partition_id = $3`

	result, err := pdb.ExecContext(ctx,
		updateNamespaceMetadataWithCASQry,
		newNotificationVersion,
		expectedNotificationVersion,
		partitionID,
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
			"namespace_metadata notification_version changed from expected %d (CAS update failed)",
			expectedNotificationVersion,
		)
	}

	return newNotificationVersion, nil
}

// UpdateNamespaceWithCAS performs a conditional update on the namespaces table
// using notification_version as fencing token. This provides atomic namespace updates
// with proper concurrency control.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - row: The namespace row data to update
//   - expectedNotificationVersion: The expected current notification_version (fencing token)
//
// Returns:
//   - ConditionFailedError if expectedNotificationVersion doesn't match current value
//   - Other errors for database failures
//
// Usage pattern:
//   1. Read current namespace data including notification_version
//   2. Modify the namespace data as needed
//   3. Call UpdateNamespaceWithCAS with current notification_version as fencing token
//   4. Handle ConditionFailedError as ownership lost (normal under contention)
func (pdb *db) UpdateNamespaceWithCAS(
	ctx context.Context,
	row *sqlplugin.NamespaceRow,
	expectedNotificationVersion int64,
) error {
	// Convert UUID to string as otherwise pgx would write the UUID as []byte
	idStr := row.ID.String()
	
	const updateNamespaceWithCASQry = `UPDATE namespaces 
		SET name = $1, data = $2, data_encoding = $3, is_global = $4, notification_version = $5
		WHERE partition_id = $6 AND id = $7 AND notification_version = $8`

	result, err := pdb.ExecContext(ctx,
		updateNamespaceWithCASQry,
		row.Name,
		row.Data,
		row.DataEncoding,
		row.IsGlobal,
		row.NotificationVersion,
		partitionID,
		idStr,
		expectedNotificationVersion,
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
			"namespace %s notification_version changed from expected %d (CAS update failed)",
			row.Name, expectedNotificationVersion,
		)
	}

	return nil
}

// UpdateNamespaceWithFencing performs a fenced update on namespaces table using notification_version as fencing token
// This is the safe method for updating namespaces in DSQL's optimistic concurrency model
func (pdb *db) UpdateNamespaceWithFencing(
	ctx context.Context,
	row *sqlplugin.NamespaceRow,
	expectedNotificationVersion int64,
) (sql.Result, error) {
	// Log the row data for debugging
	fmt.Printf("[DSQL-DEBUG] UpdateNamespaceWithFencing: ID=%s, Name=%s, DataType=%T, DataLen=%d, DataEncoding=%s, DataHex=%x, ExpectedVersion=%d\n",
		row.ID, row.Name, row.Data, len(row.Data), row.DataEncoding, row.Data, expectedNotificationVersion)
	
	err := pdb.UpdateNamespaceWithCAS(ctx, row, expectedNotificationVersion)
	if err != nil {
		return nil, err
	}
	
	// Return a dummy result since UpdateNamespaceWithCAS doesn't return sql.Result
	// This maintains interface compatibility
	return &dummyResult{rowsAffected: 1}, nil
}
// dummyResult implements sql.Result for interface compatibility
type dummyResult struct {
	rowsAffected int64
}

func (r *dummyResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *dummyResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}