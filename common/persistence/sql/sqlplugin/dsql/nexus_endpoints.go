package dsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// bytesToUUIDString safely converts a byte slice to UUID string format
// Handles empty slices and slices shorter than 16 bytes
func bytesToUUIDString(id []byte) string {
	if len(id) == 0 {
		// Return empty string for empty ID (used in pagination)
		return ""
	}
	if len(id) < 16 {
		// Pad with zeros if too short
		padded := make([]byte, 16)
		copy(padded, id)
		id = padded
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

const (
	createEndpointsTableVersionQry    = `INSERT INTO nexus_endpoints_partition_status(version) VALUES(1)`
	incrementEndpointsTableVersionQry = `UPDATE nexus_endpoints_partition_status SET version = $1 WHERE version = $2`
	getEndpointsTableVersionQry       = `SELECT version FROM nexus_endpoints_partition_status`

	createEndpointQry  = `INSERT INTO nexus_endpoints(id, data, data_encoding, version) VALUES ($1, $2, $3, 1)`
	updateEndpointQry  = `UPDATE nexus_endpoints SET data = $1, data_encoding = $2, version = $3 WHERE id = $4 AND version = $5`
	deleteEndpointQry  = `DELETE FROM nexus_endpoints WHERE id = $1`
	getEndpointByIdQry = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id = $1`
	getEndpointsQry    = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id > $1 ORDER BY id LIMIT $2`
)

func (pdb *db) InitializeNexusEndpointsTableVersion(ctx context.Context) (sql.Result, error) {
	return pdb.ExecContext(ctx, createEndpointsTableVersionQry)
}

func (pdb *db) IncrementNexusEndpointsTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, incrementEndpointsTableVersionQry, lastKnownTableVersion+1, lastKnownTableVersion)
}

func (pdb *db) GetNexusEndpointsTableVersion(
	ctx context.Context,
) (int64, error) {
	var version int64
	err := pdb.GetContext(ctx, &version, getEndpointsTableVersionQry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (pdb *db) InsertIntoNexusEndpoints(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	// Convert UUID bytes to string for DSQL VARCHAR compatibility
	idStr := bytesToUUIDString(row.ID)
	return pdb.ExecContext(
		ctx,
		createEndpointQry,
		idStr,
		row.Data,
		row.DataEncoding)
}

func (pdb *db) UpdateNexusEndpoint(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	// Convert UUID bytes to string for DSQL VARCHAR compatibility
	idStr := bytesToUUIDString(row.ID)
	return pdb.ExecContext(
		ctx,
		updateEndpointQry,
		row.Data,
		row.DataEncoding,
		row.Version+1,
		idStr,
		row.Version)
}

func (pdb *db) DeleteFromNexusEndpoints(
	ctx context.Context,
	id []byte,
) (sql.Result, error) {
	// Convert UUID bytes to string for DSQL VARCHAR compatibility
	idStr := bytesToUUIDString(id)
	return pdb.ExecContext(ctx, deleteEndpointQry, idStr)
}

func (pdb *db) GetNexusEndpointByID(
	ctx context.Context,
	id []byte,
) (*sqlplugin.NexusEndpointsRow, error) {
	var row sqlplugin.NexusEndpointsRow
	// Convert UUID bytes to string for DSQL VARCHAR compatibility
	idStr := bytesToUUIDString(id)
	err := pdb.GetContext(ctx, &row, getEndpointByIdQry, idStr)
	return &row, err
}

func (pdb *db) ListNexusEndpoints(
	ctx context.Context,
	request *sqlplugin.ListNexusEndpointsRequest,
) ([]sqlplugin.NexusEndpointsRow, error) {
	var rows []sqlplugin.NexusEndpointsRow
	// Convert UUID bytes to string for DSQL VARCHAR compatibility
	// Handle empty LastID (first call)
	lastIDStr := bytesToUUIDString(request.LastID)
	err := pdb.SelectContext(ctx, &rows, getEndpointsQry, lastIDStr, request.Limit)
	return rows, err
}
