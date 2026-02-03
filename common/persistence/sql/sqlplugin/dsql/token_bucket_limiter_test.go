package dsql

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTokenBucketEnabled(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected bool
	}{
		{"empty", "", false},
		{"true", "true", true},
		{"false", "false", false},
		{"1", "1", true},
		{"0", "0", false},
		{"invalid", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue == "" {
				os.Unsetenv(TokenBucketEnabledEnvVar)
			} else {
				os.Setenv(TokenBucketEnabledEnvVar, tt.envValue)
			}
			defer os.Unsetenv(TokenBucketEnabledEnvVar)

			result := IsTokenBucketEnabled()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNewTokenBucketLimiter_Defaults(t *testing.T) {
	// Clear any env vars
	os.Unsetenv(TokenBucketRateEnvVar)
	os.Unsetenv(TokenBucketCapacityEnvVar)
	os.Unsetenv(TokenBucketMaxWaitEnvVar)

	limiter := NewTokenBucketLimiter(nil, "test-table", "test-endpoint.dsql.us-east-1.on.aws", nil)

	require.Equal(t, int64(DefaultTokenBucketRate), limiter.Rate)
	require.Equal(t, int64(DefaultTokenBucketCapacity), limiter.Capacity)
	require.Equal(t, DefaultTokenBucketMaxWait, limiter.MaxWait)
	require.Equal(t, "test-endpoint.dsql.us-east-1.on.aws", limiter.endpoint)
	require.Equal(t, "test-table", limiter.tableName)
}

func TestNewTokenBucketLimiter_CustomEnvVars(t *testing.T) {
	os.Setenv(TokenBucketRateEnvVar, "50")
	os.Setenv(TokenBucketCapacityEnvVar, "500")
	os.Setenv(TokenBucketMaxWaitEnvVar, "10s")
	defer func() {
		os.Unsetenv(TokenBucketRateEnvVar)
		os.Unsetenv(TokenBucketCapacityEnvVar)
		os.Unsetenv(TokenBucketMaxWaitEnvVar)
	}()

	limiter := NewTokenBucketLimiter(nil, "test-table", "test-endpoint", nil)

	require.Equal(t, int64(50), limiter.Rate)
	require.Equal(t, int64(500), limiter.Capacity)
	require.Equal(t, 10*1000*1000*1000, int(limiter.MaxWait)) // 10s in nanoseconds
}

func TestNewTokenBucketLimiter_EndpointNormalization(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		expected string
	}{
		{"lowercase", "test-endpoint", "test-endpoint"},
		{"uppercase", "TEST-ENDPOINT", "test-endpoint"},
		{"with_port", "test-endpoint:5432", "test-endpoint"},
		{"with_spaces", "  test-endpoint  ", "test-endpoint"},
		{"full_dsql_endpoint", "abc123.dsql.us-east-1.on.aws:5432", "abc123.dsql.us-east-1.on.aws"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := NewTokenBucketLimiter(nil, "test-table", tt.endpoint, nil)
			require.Equal(t, tt.expected, limiter.endpoint)
		})
	}
}

func TestTokenBucketLimiter_Wait_Disabled(t *testing.T) {
	// Test that Wait returns immediately when rate is 0
	limiter := &TokenBucketLimiter{
		Rate:     0,
		Capacity: 1000,
	}

	err := limiter.Wait(nil)
	require.NoError(t, err)

	// Test that Wait returns immediately when capacity is 0
	limiter = &TokenBucketLimiter{
		Rate:     100,
		Capacity: 0,
	}

	err = limiter.Wait(nil)
	require.NoError(t, err)
}

func TestTokenBucketLimiter_calculateRetryHint(t *testing.T) {
	limiter := &TokenBucketLimiter{
		Rate: 100, // 100 tokens/sec = 10ms per token
	}

	hint := limiter.calculateRetryHint(0)
	require.Equal(t, int64(10), hint) // 1000ms / 100 = 10ms
}
