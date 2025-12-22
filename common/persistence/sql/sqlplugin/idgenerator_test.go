package sqlplugin

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
)

func TestSnowflakeIDGenerator_NextID(t *testing.T) {
	nodeID := int64(1)
	timeSource := clock.NewRealTimeSource()

	gen, err := NewSnowflakeIDGenerator(nodeID, timeSource)
	require.NoError(t, err)
	require.NotNil(t, gen)

	ctx := context.Background()

	// Generate multiple IDs
	ids := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		id, err := gen.NextID(ctx, "test_table")
		require.NoError(t, err)
		require.Positive(t, id, "ID should be positive")

		// Check for uniqueness
		require.False(t, ids[id], "ID %d was generated twice", id)
		ids[id] = true
	}
}

func TestSnowflakeIDGenerator_Concurrent(t *testing.T) {
	nodeID := int64(1)
	timeSource := clock.NewRealTimeSource()

	gen, err := NewSnowflakeIDGenerator(nodeID, timeSource)
	require.NoError(t, err)

	ctx := context.Background()
	numGoroutines := 10
	idsPerGoroutine := 100

	var wg sync.WaitGroup
	idChan := make(chan int64, numGoroutines*idsPerGoroutine)
	errChan := make(chan error, numGoroutines*idsPerGoroutine)

	// Generate IDs concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id, err := gen.NextID(ctx, "test_table")
				if err != nil {
					errChan <- err
					return
				}
				idChan <- id
			}
		}()
	}

	wg.Wait()
	close(idChan)
	close(errChan)

	// Check for errors
	for err := range errChan {
		require.NoError(t, err)
	}

	// Check all IDs are unique
	ids := make(map[int64]bool)
	for id := range idChan {
		require.False(t, ids[id], "ID %d was generated twice", id)
		ids[id] = true
	}

	assert.Len(t, ids, numGoroutines*idsPerGoroutine)
}

func TestSnowflakeIDGenerator_Monotonic(t *testing.T) {
	nodeID := int64(1)
	timeSource := clock.NewRealTimeSource()

	gen, err := NewSnowflakeIDGenerator(nodeID, timeSource)
	require.NoError(t, err)

	ctx := context.Background()

	// Generate IDs and verify they're monotonically increasing
	prevID := int64(0)
	for i := 0; i < 100; i++ {
		id, err := gen.NextID(ctx, "test_table")
		require.NoError(t, err)
		require.Greater(t, id, prevID, "ID should be monotonically increasing")
		prevID = id
	}
}

func TestSnowflakeIDGenerator_InvalidNodeID(t *testing.T) {
	timeSource := clock.NewRealTimeSource()

	// Test negative node ID
	_, err := NewSnowflakeIDGenerator(-1, timeSource)
	require.Error(t, err)

	// Test node ID too large
	_, err = NewSnowflakeIDGenerator(maxNodeID+1, timeSource)
	require.Error(t, err)
}

func TestSnowflakeIDGenerator_SequenceRollover(t *testing.T) {
	nodeID := int64(1)

	// Use mock time source to control time
	mockTime := time.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(mockTime)

	gen, err := NewSnowflakeIDGenerator(nodeID, timeSource)
	require.NoError(t, err)

	ctx := context.Background()

	// Generate enough IDs to potentially rollover sequence
	// (maxSequence + 1 IDs in same millisecond should trigger wait)
	ids := make([]int64, maxSequence+10)
	for i := range ids {
		id, err := gen.NextID(ctx, "test_table")
		require.NoError(t, err)
		ids[i] = id

		// After maxSequence IDs, advance time to allow more IDs
		if i == int(maxSequence) {
			timeSource.Update(mockTime.Add(2 * time.Millisecond))
		}
	}

	// All IDs should be unique
	idSet := make(map[int64]bool)
	for _, id := range ids {
		require.False(t, idSet[id], "Duplicate ID generated")
		idSet[id] = true
	}
}

func TestRandomIDGenerator_NextID(t *testing.T) {
	gen := NewRandomIDGenerator()
	require.NotNil(t, gen)

	ctx := context.Background()

	// Generate multiple IDs
	ids := make(map[int64]bool)
	for i := 0; i < 1000; i++ {
		id, err := gen.NextID(ctx, "test_table")
		require.NoError(t, err)
		require.Positive(t, id, "ID should be positive")

		// Check for uniqueness (very unlikely to collide with random IDs)
		require.False(t, ids[id], "ID %d was generated twice", id)
		ids[id] = true
	}
}

func TestRandomIDGenerator_Concurrent(t *testing.T) {
	gen := NewRandomIDGenerator()
	ctx := context.Background()

	numGoroutines := 10
	idsPerGoroutine := 100

	var wg sync.WaitGroup
	idChan := make(chan int64, numGoroutines*idsPerGoroutine)
	errChan := make(chan error, numGoroutines*idsPerGoroutine)

	// Generate IDs concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				id, err := gen.NextID(ctx, "test_table")
				if err != nil {
					errChan <- err
					return
				}
				idChan <- id
			}
		}()
	}

	wg.Wait()
	close(idChan)
	close(errChan)

	// Check for errors
	for err := range errChan {
		require.NoError(t, err)
	}

	// Check all IDs are unique
	ids := make(map[int64]bool)
	for id := range idChan {
		require.False(t, ids[id], "ID %d was generated twice", id)
		ids[id] = true
	}

	assert.Len(t, ids, numGoroutines*idsPerGoroutine)
}

func TestGetNodeIDFromHostname(t *testing.T) {
	tests := []struct {
		name     string
		hostname string
	}{
		{"empty hostname", ""},
		{"simple hostname", "localhost"},
		{"fqdn", "server.example.com"},
		{"with numbers", "server123.example.com"},
		{"long hostname", "very-long-hostname-with-many-parts.subdomain.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodeID := GetNodeIDFromHostname(tt.hostname)
			assert.True(t, nodeID >= 0 && nodeID <= maxNodeID,
				"NodeID %d should be between 0 and %d", nodeID, maxNodeID)
		})
	}

	// Test consistency - same hostname should produce same node ID
	hostname := "test-server.example.com"
	nodeID1 := GetNodeIDFromHostname(hostname)
	nodeID2 := GetNodeIDFromHostname(hostname)
	assert.Equal(t, nodeID1, nodeID2, "Same hostname should produce same node ID")
}

func BenchmarkSnowflakeIDGenerator(b *testing.B) {
	gen, _ := NewSnowflakeIDGenerator(1, clock.NewRealTimeSource())
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gen.NextID(ctx, "test_table")
	}
}

func BenchmarkRandomIDGenerator(b *testing.B) {
	gen := NewRandomIDGenerator()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = gen.NextID(ctx, "test_table")
	}
}
