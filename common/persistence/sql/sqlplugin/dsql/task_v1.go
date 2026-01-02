package dsql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	getTaskMinMaxQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id=$2 AND task_id >= $3 AND task_id < $4 ` +
		`ORDER BY task_id LIMIT $5`

	getTaskMinQry = `SELECT task_id, data, data_encoding ` +
		`FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id >= $3 ORDER BY task_id LIMIT $4`

	createTaskQry = `INSERT INTO ` +
		`tasks(range_hash, task_queue_id, task_id, data, data_encoding) ` +
		`VALUES(:range_hash, :task_queue_id, :task_id, :data, :data_encoding)`

	rangeDeleteTaskQry = `DELETE FROM tasks ` +
		`WHERE range_hash = $1 AND task_queue_id = $2 AND task_id IN (SELECT task_id FROM
		 tasks WHERE range_hash = $1 AND task_queue_id = $2 AND task_id < $3 ` +
		`ORDER BY task_queue_id,task_id LIMIT $4 )`
)

// InsertIntoTasks inserts one or more rows into tasks table
func (pdb *db) InsertIntoTasks(
	ctx context.Context,
	rows []sqlplugin.TasksRow,
) (sql.Result, error) {
	// For DSQL, we need to convert UUID fields to strings
	for _, row := range rows {
		taskQueueIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", row.TaskQueueID[0:4], row.TaskQueueID[4:6], row.TaskQueueID[6:8], row.TaskQueueID[8:10], row.TaskQueueID[10:16])
		
		_, err := pdb.ExecContext(ctx,
			`INSERT INTO tasks(range_hash, task_queue_id, task_id, data, data_encoding) VALUES($1, $2, $3, $4, $5)`,
			row.RangeHash,
			taskQueueIDStr,
			row.TaskID,
			row.Data,
			row.DataEncoding,
		)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// SelectFromTasks reads one or more rows from tasks table
func (pdb *db) SelectFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) ([]sqlplugin.TasksRow, error) {
	var err error
	var rows []sqlplugin.TasksRow
	taskQueueIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", filter.TaskQueueID[0:4], filter.TaskQueueID[4:6], filter.TaskQueueID[6:8], filter.TaskQueueID[8:10], filter.TaskQueueID[10:16])
	
	switch {
	case filter.ExclusiveMaxTaskID != nil:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskMinMaxQry,
			filter.RangeHash,
			taskQueueIDStr,
			*filter.InclusiveMinTaskID,
			*filter.ExclusiveMaxTaskID,
			*filter.PageSize,
		)
	default:
		err = pdb.SelectContext(ctx,
			&rows,
			getTaskMinQry,
			filter.RangeHash,
			taskQueueIDStr,
			*filter.InclusiveMinTaskID,
			*filter.PageSize,
		)
	}
	return rows, err
}

// DeleteFromTasks deletes multiple rows from tasks table
func (pdb *db) DeleteFromTasks(
	ctx context.Context,
	filter sqlplugin.TasksFilter,
) (sql.Result, error) {
	if filter.ExclusiveMaxTaskID == nil {
		return nil, serviceerror.NewInternal("missing ExclusiveMaxTaskID parameter")
	}
	if filter.Limit == nil || *filter.Limit == 0 {
		return nil, serviceerror.NewInternal("missing limit parameter")
	}
	taskQueueIDStr := fmt.Sprintf("%x-%x-%x-%x-%x", filter.TaskQueueID[0:4], filter.TaskQueueID[4:6], filter.TaskQueueID[6:8], filter.TaskQueueID[8:10], filter.TaskQueueID[10:16])
	return pdb.ExecContext(ctx,
		rangeDeleteTaskQry,
		filter.RangeHash,
		taskQueueIDStr,
		*filter.ExclusiveMaxTaskID,
		*filter.Limit,
	)
}
