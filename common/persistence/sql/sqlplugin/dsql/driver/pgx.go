package driver

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // register pgx driver for sqlx
	"github.com/jmoiron/sqlx"
)

type PGXDriver struct{}

func (p *PGXDriver) CreateConnection(dsn string) (*sqlx.DB, error) {
	return sqlx.Connect("pgx", dsn)
}

func (p *PGXDriver) IsDupEntryError(err error) bool {
	pgxErr, ok := err.(*pgconn.PgError)
	return ok && pgxErr.Code == dupEntryCode
}

func (p *PGXDriver) IsDupDatabaseError(err error) bool {
	pqErr, ok := err.(*pgconn.PgError)
	return ok && pqErr.Code == dupDatabaseCode
}

func (p *PGXDriver) IsConnNeedsRefreshError(err error) bool {
	if err == nil {
		return false
	}

	// First, try to unwrap to *pgconn.PgError
	var pqErr *pgconn.PgError
	if errors.As(err, &pqErr) {
		return isConnNeedsRefreshError(pqErr.Code, pqErr.Message)
	}

	// For connection errors that aren't wrapped as PgError,
	// check the error string for DSQL-specific auth failure patterns.
	// pgx connection errors include the SQLSTATE in the message like:
	// "server error: FATAL: unable to accept connection, access denied (SQLSTATE 08006)"
	errStr := strings.ToLower(err.Error())

	// Check for DSQL IAM authentication failures
	if strings.Contains(errStr, "access denied") ||
		strings.Contains(errStr, "unable to accept connection") {
		return true
	}

	// Check for connection exception SQLSTATE codes in the error message
	// These indicate the connection is broken and needs refresh
	if strings.Contains(errStr, "sqlstate 08006") || // connection_failure
		strings.Contains(errStr, "sqlstate 08000") || // connection_exception
		strings.Contains(errStr, "sqlstate 08003") || // connection_does_not_exist
		strings.Contains(errStr, "sqlstate 08001") { // sqlclient_unable_to_establish_sqlconnection
		return true
	}

	return false
}
