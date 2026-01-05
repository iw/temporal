package dsql

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

// createPgError creates a pgconn.PgError for testing purposes
func createPgError(sqlState, message string) *pgconn.PgError {
	return &pgconn.PgError{
		Severity:         "ERROR",
		Code:             sqlState,
		Message:          message,
		Detail:           "",
		Hint:             "",
		Position:         0,
		InternalPosition: 0,
		InternalQuery:    "",
		Where:            "",
		SchemaName:       "",
		TableName:        "",
		ColumnName:       "",
		DataTypeName:     "",
		ConstraintName:   "",
		File:             "",
		Line:             0,
		Routine:          "",
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected ErrorType
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: ErrorTypeUnknown,
		},
		{
			name:     "condition failed error",
			err:      NewConditionFailedError("test condition failed"),
			expected: ErrorTypeConditionFailed,
		},
		{
			name:     "serialization failure",
			err:      createPgError("40001", "serialization failure"),
			expected: ErrorTypeRetryable,
		},
		{
			name:     "unsupported feature",
			err:      createPgError("0A000", "feature not supported"),
			expected: ErrorTypeUnsupportedFeature,
		},
		{
			name:     "other postgres error",
			err:      createPgError("23505", "unique violation"),
			expected: ErrorTypePermanent,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: ErrorTypePermanent,
		},
		{
			name:     "generic error",
			err:      errors.New("generic error"),
			expected: ErrorTypePermanent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ClassifyError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "condition failed error - not retryable",
			err:      NewConditionFailedError("test condition failed"),
			expected: false,
		},
		{
			name:     "serialization failure - retryable",
			err:      createPgError("40001", "serialization failure"),
			expected: true,
		},
		{
			name:     "unsupported feature - not retryable",
			err:      createPgError("0A000", "feature not supported"),
			expected: false,
		},
		{
			name:     "other postgres error - not retryable",
			err:      createPgError("23505", "unique violation"),
			expected: false,
		},
		{
			name:     "context deadline exceeded - not retryable",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "generic error - not retryable",
			err:      errors.New("generic error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestWrapWithConditionFailed(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		msg      string
		args     []interface{}
		expected string
	}{
		{
			name:     "wrap nil error",
			err:      nil,
			msg:      "test message",
			expected: "condition failed: test message",
		},
		{
			name:     "wrap existing error",
			err:      errors.New("original error"),
			msg:      "test message",
			expected: "condition failed: test message: original error",
		},
		{
			name:     "wrap with format args",
			err:      nil,
			msg:      "test message %s %d",
			args:     []interface{}{"arg1", 42},
			expected: "condition failed: test message arg1 42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := WrapWithConditionFailed(tt.err, tt.msg, tt.args...)
			assert.Equal(t, tt.expected, result.Error())
			assert.True(t, IsConditionFailedError(result))
		})
	}
}