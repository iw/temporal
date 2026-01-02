package dsql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// below are templates for history_node table
	addHistoryNodesQuery = `INSERT INTO history_node (` +
		`shard_id, tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :node_id, :prev_txn_id, :txn_id, :data, :data_encoding) ` +
		`ON CONFLICT (shard_id, tree_id, branch_id, node_id, txn_id) DO ` +
		`UPDATE SET prev_txn_id=:prev_txn_id, data=:data, data_encoding=:data_encoding `

	getHistoryNodesQuery = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND ((node_id = $4 AND txn_id > $5) OR node_id > $6) AND node_id < $7 ` +
		`ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT $8 `

	getHistoryNodesReverseQuery = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND node_id >= $4 AND ((node_id = $5 AND txn_id < $6) OR node_id < $7) ` +
		`ORDER BY shard_id, tree_id, branch_id DESC, node_id DESC, txn_id DESC LIMIT $8 `

	getHistoryNodeMetadataQuery = `SELECT node_id, prev_txn_id, txn_id FROM history_node ` +
		`WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND ((node_id = $4 AND txn_id > $5) OR node_id > $6) AND node_id < $7 ` +
		`ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT $8 `

	deleteHistoryNodeQuery = `DELETE FROM history_node WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND node_id = $4 AND txn_id = $5 `

	deleteHistoryNodesQuery = `DELETE FROM history_node WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND node_id >= $4 `

	// below are templates for history_tree table
	addHistoryTreeQuery = `INSERT INTO history_tree (` +
		`shard_id, tree_id, branch_id, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :data, :data_encoding) ` +
		`ON CONFLICT (shard_id, tree_id, branch_id) DO UPDATE ` +
		`SET data = excluded.data, data_encoding = excluded.data_encoding`

	getHistoryTreeQuery = `SELECT branch_id, data, data_encoding FROM history_tree WHERE shard_id = $1 AND tree_id = $2 `

	paginateBranchesQuery = `SELECT shard_id, tree_id, branch_id, data, data_encoding
        FROM history_tree
        WHERE (shard_id, tree_id, branch_id) > ($1, $2, $3)
        ORDER BY shard_id, tree_id, branch_id
        LIMIT $4`

	deleteHistoryTreeQuery = `DELETE FROM history_tree WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 `
)

// For history_node table:

// InsertIntoHistoryNode inserts a row into history_node table
func (pdb *db) InsertIntoHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	row.TxnID = -row.TxnID
	
	// DSQL-specific: Convert UUID bytes to strings for VARCHAR fields
	originalTreeID := row.TreeID
	originalBranchID := row.BranchID
	
	// Convert UUID bytes to string representation for DSQL
	if len(row.TreeID) == 16 {
		// This is a UUID in byte format, convert to string
		treeIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", 
			row.TreeID[0:4], row.TreeID[4:6], row.TreeID[6:8], row.TreeID[8:10], row.TreeID[10:16])
		row.TreeID = []byte(treeIDStr)
	}
	if len(row.BranchID) == 16 {
		// This is a UUID in byte format, convert to string
		branchIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", 
			row.BranchID[0:4], row.BranchID[4:6], row.BranchID[6:8], row.BranchID[8:10], row.BranchID[10:16])
		row.BranchID = []byte(branchIDStr)
	}
	
	result, err := pdb.NamedExecContext(ctx, addHistoryNodesQuery, row)
	
	// Restore original values to avoid side effects
	row.TreeID = originalTreeID
	row.BranchID = originalBranchID
	row.TxnID = -row.TxnID // Restore original txn_id
	
	return result, err
}

// DeleteFromHistoryNode delete a row from history_node table
func (pdb *db) DeleteFromHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	row.TxnID = -row.TxnID
	return pdb.ExecContext(ctx,
		deleteHistoryNodeQuery,
		row.ShardID,
		row.TreeID,
		row.BranchID,
		row.NodeID,
		row.TxnID,
	)
}

// SelectFromHistoryNode reads one or more rows from history_node table
func (pdb *db) RangeSelectFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeSelectFilter,
) ([]sqlplugin.HistoryNodeRow, error) {
	var query string
	if filter.MetadataOnly {
		query = getHistoryNodeMetadataQuery
	} else if filter.ReverseOrder {
		query = getHistoryNodesReverseQuery
	} else {
		query = getHistoryNodesQuery
	}

	var args []interface{}
	if filter.ReverseOrder {
		args = []interface{}{
			filter.ShardID,
			filter.TreeID,
			filter.BranchID,
			filter.MinNodeID,
			filter.MaxTxnID,
			-filter.MaxTxnID,
			filter.MaxNodeID,
			filter.PageSize,
		}
	} else {
		args = []interface{}{
			filter.ShardID,
			filter.TreeID,
			filter.BranchID,
			filter.MinNodeID,
			-filter.MinTxnID, // NOTE: transaction ID is *= -1 when stored
			filter.MinNodeID,
			filter.MaxNodeID,
			filter.PageSize,
		}
	}

	var rows []sqlplugin.HistoryNodeRow
	err := pdb.SelectContext(ctx, &rows, query, args...)
	if err != nil {
		return nil, err
	}
	// NOTE: since we let txn_id multiple by -1 when inserting, we have to revert it back here
	for index := range rows {
		rows[index].TxnID = -rows[index].TxnID
	}
	return rows, nil
}

// DeleteFromHistoryNode deletes one or more rows from history_node table
func (pdb *db) RangeDeleteFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeDeleteFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteHistoryNodesQuery,
		filter.ShardID,
		filter.TreeID,
		filter.BranchID,
		filter.MinNodeID,
	)
}

// For history_tree table:

// InsertIntoHistoryTree inserts a row into history_tree table
func (pdb *db) InsertIntoHistoryTree(
	ctx context.Context,
	row *sqlplugin.HistoryTreeRow,
) (sql.Result, error) {
	// DSQL-specific: Convert UUID bytes to strings for VARCHAR fields
	originalTreeID := row.TreeID
	originalBranchID := row.BranchID
	
	// Convert UUID bytes to string representation for DSQL
	if len(row.TreeID) == 16 {
		// This is a UUID in byte format, convert to string
		treeIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", 
			row.TreeID[0:4], row.TreeID[4:6], row.TreeID[6:8], row.TreeID[8:10], row.TreeID[10:16])
		row.TreeID = []byte(treeIDStr)
	}
	if len(row.BranchID) == 16 {
		// This is a UUID in byte format, convert to string
		branchIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", 
			row.BranchID[0:4], row.BranchID[4:6], row.BranchID[6:8], row.BranchID[8:10], row.BranchID[10:16])
		row.BranchID = []byte(branchIDStr)
	}
	
	result, err := pdb.NamedExecContext(ctx, addHistoryTreeQuery, row)
	
	// Restore original values to avoid side effects
	row.TreeID = originalTreeID
	row.BranchID = originalBranchID
	
	return result, err
}

// SelectFromHistoryTree reads one or more rows from history_tree table
func (pdb *db) SelectFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeSelectFilter,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := pdb.SelectContext(ctx,
		&rows,
		getHistoryTreeQuery,
		filter.ShardID,
		filter.TreeID,
	)
	return rows, err
}

// PaginateBranchesFromHistoryTree reads up to page.Limit rows from the history_tree table sorted by their primary key,
// while skipping the first page.Offset rows.
func (pdb *db) PaginateBranchesFromHistoryTree(
	ctx context.Context,
	page sqlplugin.HistoryTreeBranchPage,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := pdb.SelectContext(ctx,
		&rows,
		paginateBranchesQuery,
		page.ShardID,
		page.TreeID,
		page.BranchID,
		page.Limit,
	)
	return rows, err
}

// DeleteFromHistoryTree deletes one or more rows from history_tree table
func (pdb *db) DeleteFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeDeleteFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteHistoryTreeQuery,
		filter.ShardID,
		filter.TreeID,
		filter.BranchID,
	)
}
