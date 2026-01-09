package driver

import (
	"strings"

	"github.com/jmoiron/sqlx"
)

const (
	// check http://www.postgresql.org/docs/9.3/static/errcodes-appendix.html
	dupEntryCode            = "23505"
	dupDatabaseCode         = "42P04"
	readOnlyTransactionCode = "25006"
	cannotConnectNowCode    = "57P03"
	featureNotSupportedCode = "0A000"

	// Connection exception codes (Class 08 - Connection Exception)
	// These indicate the connection is broken and needs to be refreshed
	// Critical for DSQL IAM authentication token expiration handling
	connectionExceptionCode              = "08000" // connection_exception (general)
	connectionDoesNotExistCode           = "08003" // connection_does_not_exist
	connectionFailureCode                = "08006" // connection_failure (includes IAM auth "access denied")
	sqlclientUnableToEstablishConnection = "08001" // sqlclient_unable_to_establish_sqlconnection

	// Unsupported "feature" messages to look for
	cannotSetReadWriteModeDuringRecoveryMsg = "cannot set transaction read-write mode during recovery"
)

type Driver interface {
	CreateConnection(dsn string) (*sqlx.DB, error)
	IsDupEntryError(error) bool
	IsDupDatabaseError(error) bool
	IsConnNeedsRefreshError(error) bool
}

func isConnNeedsRefreshError(code, message string) bool {
	// PostgreSQL read-only and cannot-connect-now errors
	if code == readOnlyTransactionCode || code == cannotConnectNowCode {
		return true
	}

	// Connection exception class (08xxx) - connection is broken/unusable
	// This is critical for DSQL IAM authentication:
	// When IAM tokens expire, DSQL returns 08006 with "access denied"
	// We need to refresh the connection to generate a new IAM token
	if code == connectionExceptionCode ||
		code == connectionDoesNotExistCode ||
		code == connectionFailureCode ||
		code == sqlclientUnableToEstablishConnection {
		return true
	}

	// Also check for "access denied" in message for any connection-related error
	// This catches edge cases where DSQL might use different error codes
	if strings.Contains(strings.ToLower(message), "access denied") ||
		strings.Contains(strings.ToLower(message), "unable to accept connection") {
		return true
	}

	if code == featureNotSupportedCode && message == cannotSetReadWriteModeDuringRecoveryMsg {
		return true
	}
	return false
}
