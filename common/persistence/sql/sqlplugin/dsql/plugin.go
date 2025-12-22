package dsql

import (
	"fmt"
	"net"
	"strings"

	"github.com/lib/pq"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
)

const (
	// PluginName is the registered name for the DSQL persistence plugin.
	PluginName = "dsql"
	// PostgreSQL plugin name to delegate to
	PostgreSQLPluginName = "postgres12"
)

func init() {
	// Register DSQL as an alias to PostgreSQL plugin
	// This provides the base functionality while allowing for DSQL-specific customizations
	sql.RegisterPluginAlias(PluginName, PostgreSQLPluginName)
}

// ConvertError converts DSQL-specific errors to appropriate types
// This function can be used by the persistence layer to handle DSQL-specific errors
func ConvertError(err error) error {
	if err == nil {
		return nil
	}

	// Handle DSQL-specific errors
	if pqErr, ok := err.(*pq.Error); ok {
		switch pqErr.Code {
		case "40001": // serialization_failure - DSQL uses optimistic concurrency
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("DSQL serialization conflict: %s", pqErr.Message),
			}
		case "23505": // unique_violation
			return &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("DSQL unique constraint violation: %s", pqErr.Message),
			}
		}
	}

	// Check for connection errors that require refresh
	if isConnectionError(err) {
		return fmt.Errorf("DSQL connection error: %w", err)
	}

	// Return the original error for other cases
	return err
}

// isConnectionError checks if the error indicates a connection problem
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// DSQL-specific connection error patterns
	dsqlErrors := []string{
		"dsql cluster unavailable",
		"dsql endpoint not found",
		"connection to dsql failed",
		"dsql authentication failed",
	}

	for _, pattern := range dsqlErrors {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	// Standard connection errors
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "no such host")
}
