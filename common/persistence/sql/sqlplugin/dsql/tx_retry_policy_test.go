package dsql

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDSQLTxRetryPolicy_Creation(t *testing.T) {
	cfg := RetryConfig{
		MaxRetries:   3,
		BaseDelay:    50 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		JitterFactor: 0.1,
	}
	rm := &RetryManager{config: cfg}
	
	policy := NewDSQLTxRetryPolicy(rm)
	require.NotNil(t, policy)
	
	dsqlPolicy, ok := policy.(*dsqlTxRetryPolicy)
	require.True(t, ok)
	
	assert.Equal(t, 3, dsqlPolicy.MaxRetries())
	assert.Equal(t, 50*time.Millisecond, dsqlPolicy.baseDelay)
	assert.Equal(t, 2*time.Second, dsqlPolicy.maxDelay)
	assert.Equal(t, 0.1, dsqlPolicy.jitterFactor)
}

func TestDSQLTxRetryPolicy_MaxRetries(t *testing.T) {
	cfg := RetryConfig{MaxRetries: 5}
	rm := &RetryManager{config: cfg}
	policy := NewDSQLTxRetryPolicy(rm)
	
	assert.Equal(t, 5, policy.MaxRetries())
}

func TestDSQLTxRetryPolicy_Backoff(t *testing.T) {
	cfg := RetryConfig{
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		JitterFactor: 0, // No jitter for predictable testing
	}
	rm := &RetryManager{config: cfg}
	policy := NewDSQLTxRetryPolicy(rm)
	
	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1 * time.Second}, // Capped at maxDelay
		{5, 1 * time.Second}, // Still capped
	}
	
	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			delay := policy.Backoff(tt.attempt)
			assert.Equal(t, tt.expected, delay)
		})
	}
}

func TestDSQLTxRetryPolicy_RetryableTxError(t *testing.T) {
	cfg := DefaultRetryConfig()
	rm := &RetryManager{config: cfg}
	policy := NewDSQLTxRetryPolicy(rm)
	
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
			err:      NewConditionFailedError(ConditionFailedShard, "test"),
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
			name:     "generic error - not retryable",
			err:      errors.New("generic error"),
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := policy.RetryableTxError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDSQLTxRetryPolicy_BackoffWithJitter(t *testing.T) {
	cfg := RetryConfig{
		BaseDelay:    100 * time.Millisecond,
		MaxDelay:     1 * time.Second,
		JitterFactor: 0.25, // 25% jitter
	}
	rm := &RetryManager{config: cfg}
	policy := NewDSQLTxRetryPolicy(rm)
	
	// Test that jitter produces different values
	delays := make([]time.Duration, 10)
	for i := 0; i < 10; i++ {
		delays[i] = policy.Backoff(1) // attempt 1 should be ~200ms with jitter
	}
	
	// Check that we get some variation (not all the same)
	allSame := true
	for i := 1; i < len(delays); i++ {
		if delays[i] != delays[0] {
			allSame = false
			break
		}
	}
	assert.False(t, allSame, "Expected jitter to produce different delay values")
	
	// Check that all delays are within reasonable bounds (base ± jitter)
	baseDelay := 200 * time.Millisecond // 2^1 * 100ms
	minDelay := time.Duration(float64(baseDelay) * 0.75) // -25% jitter
	maxDelay := time.Duration(float64(baseDelay) * 1.25) // +25% jitter
	
	for i, delay := range delays {
		assert.GreaterOrEqual(t, delay, minDelay, "Delay %d too small: %v", i, delay)
		assert.LessOrEqual(t, delay, maxDelay, "Delay %d too large: %v", i, delay)
	}
}