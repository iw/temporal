package dsql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// DSQL-specific error classification utilities
// 
// NOTE: This module provides error CLASSIFICATION only, not conversion.
// Error conversion is handled by Temporal's standard handle.ConvertError().
// We only classify errors to determine retry behavior while preserving
// the original error structure for proper SQLSTATE detection.

// ClassifyError provides canonical error classification for DSQL operations.
// This is the authoritative error classifier used by the retry logic.
// 
// CRITICAL: This function does NOT convert errors - it only classifies them.
// The original error structure is preserved so SQLSTATE detection works correctly.
func ClassifyError(err error) ErrorType {
	if err == nil {
		return ErrorTypeUnknown
	}

	// CRITICAL: Never retry condition failures - check first
	if IsConditionFailedError(err) {
		return ErrorTypeConditionFailed
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.SQLState() {
		case "40001": // serialization_failure - DSQL serialization conflict
			// IMPORTANT: We classify this as retryable but do NOT convert it here.
			// The retry logic needs to detect SQLSTATE 40001 directly.
			return ErrorTypeRetryable
		case "0A000": // feature_not_supported - unsupported SQL feature
			return ErrorTypeUnsupportedFeature
		default:
			return ErrorTypePermanent
		}
	}

	// Don't retry timeouts - they indicate system overload
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrorTypePermanent
	}

	// Don't retry connection errors for now - may add specific cases later
	return ErrorTypePermanent
}

// IsRetryableError determines if a DSQL error should be retried.
// This function uses the canonical error classification to determine retryability.
func IsRetryableError(err error) bool {
	return ClassifyError(err) == ErrorTypeRetryable
}

// LogError logs DSQL errors with appropriate severity and context.
// This function should be used by the retry manager and other components for consistent logging.
func LogError(logger log.Logger, err error, operation string, attempt int) {
	if err == nil || logger == nil {
		return
	}

	errorType := ClassifyError(err)
	
	// Create base tags
	tags := []tag.Tag{
		tag.NewStringTag("operation", operation),
		tag.NewStringTag("error_type", errorType.String()),
		tag.Error(err),
	}
	
	if attempt > 0 {
		tags = append(tags, tag.NewInt("attempt", attempt))
	}

	switch errorType {
	case ErrorTypeRetryable:
		// Log retryable errors at debug level to avoid noise
		logger.Debug("DSQL retryable error", tags...)
	case ErrorTypeConditionFailed:
		// Log condition failures at debug level - they're normal under contention
		logger.Debug("DSQL condition failed (normal under contention)", tags...)
	case ErrorTypeUnsupportedFeature:
		// Log unsupported features at error level - these are implementation bugs
		logger.Error("DSQL unsupported feature (implementation bug)", tags...)
	case ErrorTypePermanent:
		// Log permanent errors at warn level
		logger.Warn("DSQL permanent error", tags...)
	default:
		// Log unknown errors at warn level
		logger.Warn("DSQL unknown error", tags...)
	}
}

// WrapWithConditionFailed wraps an error as a condition failed error.
// This is a utility function for creating condition failed errors with context.
func WrapWithConditionFailed(err error, msg string, args ...interface{}) error {
	if err == nil {
		return NewConditionFailedError(msg, args...)
	}
	return NewConditionFailedError("%s: %v", fmt.Sprintf(msg, args...), err)
}

// RecordErrorMetrics records error metrics for DSQL operations.
// This function should be called by the retry manager to track error patterns.
func RecordErrorMetrics(metrics DSQLMetrics, err error, operation string) {
	if metrics == nil || err == nil {
		return
	}

	errorType := ClassifyError(err)
	metrics.IncTxErrorClass(operation, errorType.String())
	
	// Record specific error patterns for monitoring
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Track specific SQLSTATE patterns
		switch pgErr.SQLState() {
		case "40001":
			metrics.IncTxConflict(operation)
		case "0A000":
			// Track unsupported feature usage - these are bugs
			metrics.IncTxErrorClass(operation, "unsupported_feature_bug")
		}
	}
}

// isConnectionError checks if the error indicates a connection problem
// This is used internally for connection health monitoring
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
		"iam authentication failed",
		"token expired",
		"invalid token",
	}

	for _, pattern := range dsqlErrors {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	// Standard connection errors
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "no such host") ||
		strings.Contains(errStr, "timeout")
}