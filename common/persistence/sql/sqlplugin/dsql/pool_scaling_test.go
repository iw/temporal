package dsql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCalculateStaggerDuration(t *testing.T) {
	tests := []struct {
		name     string
		poolSize int
		expected time.Duration
	}{
		{
			name:     "small pool (10) - no stagger",
			poolSize: 10,
			expected: 0,
		},
		{
			name:     "small pool (100) - no stagger",
			poolSize: 100,
			expected: 0,
		},
		{
			name:     "medium pool (200) - 2 minutes",
			poolSize: 200,
			expected: 2 * time.Minute,
		},
		{
			name:     "medium pool (500) - 2 minutes",
			poolSize: 500,
			expected: 2 * time.Minute,
		},
		{
			name:     "large pool (501) - 3 minutes minimum",
			poolSize: 501,
			expected: 3 * time.Minute,
		},
		{
			name:     "large pool (1000) - 3 minutes",
			poolSize: 1000,
			expected: 3 * time.Minute, // 1000/50 = 20s, rounds up to 1 min, but min is 3
		},
		{
			name:     "very large pool (5000) - 3 minutes",
			poolSize: 5000,
			expected: 3 * time.Minute, // 5000/50 = 100s = 1.67 min, rounds to 2 min, but min is 3
		},
		{
			name:     "huge pool (8000) - 3 minutes",
			poolSize: 8000,
			expected: 3 * time.Minute, // 8000/50 = 160s = 2.67 min, rounds to 3 min
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateStaggerDuration(tt.poolSize)
			require.Equal(t, tt.expected, result, "pool size %d", tt.poolSize)
		})
	}
}

func TestDefaultPoolKeeperConfig_Scaling(t *testing.T) {
	tests := []struct {
		name                string
		poolSize            int
		expectedMaxPerTick  int
		expectedTickSeconds int
	}{
		{
			name:                "small pool (10)",
			poolSize:            10,
			expectedMaxPerTick:  1, // 10/120 = 0.08, rounds to 1
			expectedTickSeconds: 1,
		},
		{
			name:                "medium pool (100)",
			poolSize:            100,
			expectedMaxPerTick:  1, // 100/120 = 0.83, rounds to 1
			expectedTickSeconds: 1,
		},
		{
			name:                "medium pool (500)",
			poolSize:            500,
			expectedMaxPerTick:  5, // 500/120 = 4.17, rounds to 5
			expectedTickSeconds: 1,
		},
		{
			name:                "large pool (1000)",
			poolSize:            1000,
			expectedMaxPerTick:  9, // 1000/120 = 8.33, rounds to 9
			expectedTickSeconds: 1,
		},
		{
			name:                "very large pool (2000)",
			poolSize:            2000,
			expectedMaxPerTick:  10, // 2000/120 = 16.67, capped at 10
			expectedTickSeconds: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultPoolKeeperConfig(tt.poolSize)
			require.Equal(t, tt.poolSize, cfg.TargetPoolSize)
			require.Equal(t, tt.expectedMaxPerTick, cfg.MaxConnsPerTick, "MaxConnsPerTick for pool size %d", tt.poolSize)
			require.Equal(t, time.Duration(tt.expectedTickSeconds)*time.Second, cfg.TickInterval)
		})
	}
}

func TestPoolKeeperConfig_RateLimitCompliance(t *testing.T) {
	// Verify that even with max settings, we don't exceed cluster rate limit
	// MaxConnsPerTick is capped at 10, tick interval is 1 second
	// So max local rate is 10/sec, well under 100/sec cluster limit
	const clusterRateLimit = 100 // DSQL cluster-wide limit

	cfg := DefaultPoolKeeperConfig(10000) // Very large pool
	maxLocalRate := float64(cfg.MaxConnsPerTick) / cfg.TickInterval.Seconds()

	require.LessOrEqual(t, maxLocalRate, float64(clusterRateLimit),
		"Local rate (%v/sec) should not exceed cluster rate limit (%v/sec)",
		maxLocalRate, clusterRateLimit)

	// With cap of 10/sec per service, even 10 services = 100/sec = cluster limit
	require.LessOrEqual(t, cfg.MaxConnsPerTick, 10,
		"MaxConnsPerTick should be capped to allow multiple services")
}

func TestStaggerDuration_RateLimitCompliance(t *testing.T) {
	// Verify that stagger duration allows warmup within rate limit

	testCases := []struct {
		name     string
		poolSize int
	}{
		{"pool_100", 100},
		{"pool_500", 500},
		{"pool_1000", 1000},
		{"pool_5000", 5000},
		{"pool_8000", 8000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stagger := CalculateStaggerDuration(tc.poolSize)

			if stagger == 0 {
				// Small pools don't need stagger
				require.LessOrEqual(t, tc.poolSize, 100)
				return
			}

			// Calculate warmup rate
			warmupRate := float64(tc.poolSize) / stagger.Seconds()

			// Should be well under cluster rate limit (we use 50% headroom)
			require.LessOrEqual(t, warmupRate, float64(ClusterRateLimitForWarmup)*1.5,
				"Pool %d with stagger %v has warmup rate %v/sec, should be under %v/sec",
				tc.poolSize, stagger, warmupRate, ClusterRateLimitForWarmup*1.5)
		})
	}
}
