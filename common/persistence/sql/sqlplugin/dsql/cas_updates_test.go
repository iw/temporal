package dsql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateRowsAffected(t *testing.T) {
	tests := []struct {
		name           string
		rowsAffected   int64
		entityDesc     string
		fencingToken   interface{}
		expectError    bool
		expectCASError bool
	}{
		{
			name:           "success - one row affected",
			rowsAffected:   1,
			entityDesc:     "test_entity",
			fencingToken:   123,
			expectError:    false,
			expectCASError: false,
		},
		{
			name:           "condition failed - zero rows affected",
			rowsAffected:   0,
			entityDesc:     "test_entity",
			fencingToken:   123,
			expectError:    true,
			expectCASError: true,
		},
		{
			name:           "unexpected - multiple rows affected",
			rowsAffected:   2,
			entityDesc:     "test_entity",
			fencingToken:   123,
			expectError:    true,
			expectCASError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock result
			result := &mockResult{rowsAffected: tt.rowsAffected}
			
			err := ValidateRowsAffected(result, tt.entityDesc, tt.fencingToken)
			
			if tt.expectError {
				require.Error(t, err)
				if tt.expectCASError {
					assert.True(t, IsConditionFailedError(err), "Expected ConditionFailedError")
				} else {
					assert.False(t, IsConditionFailedError(err), "Expected non-CAS error")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCASUpdateHelpers(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "condition failed error",
			err:      NewConditionFailedError("test"),
			expected: true,
		},
		{
			name:     "regular error",
			err:      assert.AnError,
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "sql error",
			err:      sql.ErrNoRows,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsConditionFailedError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// mockResult implements sql.Result for testing
type mockResult struct {
	rowsAffected int64
	lastInsertId int64
	err          error
}

func (m *mockResult) LastInsertId() (int64, error) {
	return m.lastInsertId, m.err
}

func (m *mockResult) RowsAffected() (int64, error) {
	return m.rowsAffected, m.err
}

func TestGenericCASUpdate_ErrorHandling(t *testing.T) {
	// Test the error handling logic without requiring a real database connection
	
	// Test that ValidateRowsAffected works correctly for CAS operations
	mockResult := &mockResult{rowsAffected: 0}
	err := ValidateRowsAffected(mockResult, "test_entity", 123)
	
	// Should get a condition failed error
	assert.Error(t, err)
	assert.True(t, IsConditionFailedError(err))
	assert.Contains(t, err.Error(), "test_entity fencing token changed from expected 123")
}