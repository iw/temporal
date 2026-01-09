package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsConnNeedsRefreshError(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		message  string
		expected bool
	}{
		// Original PostgreSQL error codes
		{
			name:     "read only transaction",
			code:     "25006",
			message:  "cannot execute in read-only transaction",
			expected: true,
		},
		{
			name:     "cannot connect now",
			code:     "57P03",
			message:  "the database system is starting up",
			expected: true,
		},
		{
			name:     "feature not supported during recovery",
			code:     "0A000",
			message:  "cannot set transaction read-write mode during recovery",
			expected: true,
		},
		{
			name:     "feature not supported other",
			code:     "0A000",
			message:  "some other unsupported feature",
			expected: false,
		},

		// DSQL IAM authentication error codes (Class 08 - Connection Exception)
		{
			name:     "connection exception general",
			code:     "08000",
			message:  "connection exception",
			expected: true,
		},
		{
			name:     "connection does not exist",
			code:     "08003",
			message:  "connection does not exist",
			expected: true,
		},
		{
			name:     "connection failure - IAM access denied",
			code:     "08006",
			message:  "FATAL: unable to accept connection, access denied",
			expected: true,
		},
		{
			name:     "connection failure - generic",
			code:     "08006",
			message:  "connection failure",
			expected: true,
		},
		{
			name:     "sqlclient unable to establish connection",
			code:     "08001",
			message:  "could not connect to server",
			expected: true,
		},

		// Message-based detection (fallback for edge cases)
		{
			name:     "access denied in message - different code",
			code:     "28000", // invalid_authorization_specification
			message:  "access denied for user",
			expected: true,
		},
		{
			name:     "unable to accept connection in message",
			code:     "XX000", // internal_error
			message:  "unable to accept connection due to rate limiting",
			expected: true,
		},

		// Non-refresh errors
		{
			name:     "unique violation",
			code:     "23505",
			message:  "duplicate key value violates unique constraint",
			expected: false,
		},
		{
			name:     "serialization failure",
			code:     "40001",
			message:  "could not serialize access",
			expected: false,
		},
		{
			name:     "syntax error",
			code:     "42601",
			message:  "syntax error at or near",
			expected: false,
		},
		{
			name:     "empty code and message",
			code:     "",
			message:  "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConnNeedsRefreshError(tt.code, tt.message)
			assert.Equal(t, tt.expected, result, "code=%s, message=%s", tt.code, tt.message)
		})
	}
}
