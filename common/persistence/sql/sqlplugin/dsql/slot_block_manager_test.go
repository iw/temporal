package dsql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlotBlockManager_AcquireRelease(t *testing.T) {
	// This test verifies the local slot tracking logic without DynamoDB
	// Full integration tests would require a DynamoDB mock or local instance

	t.Run("acquire increments used slots", func(t *testing.T) {
		mgr := &SlotBlockManager{
			totalSlots:  100,
			ownedBlocks: map[int]bool{0: true},
			stopC:       make(chan struct{}),
			metrics:     &noOpSlotBlockMetrics{},
		}

		leaseID, err := mgr.Acquire(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, leaseID)
		assert.Equal(t, int64(1), mgr.UsedSlots())
		assert.Equal(t, 99, mgr.AvailableSlots())
	})

	t.Run("release decrements used slots", func(t *testing.T) {
		mgr := &SlotBlockManager{
			totalSlots:  100,
			ownedBlocks: map[int]bool{0: true},
			stopC:       make(chan struct{}),
			metrics:     &noOpSlotBlockMetrics{},
		}

		leaseID, err := mgr.Acquire(context.Background())
		require.NoError(t, err)
		assert.Equal(t, int64(1), mgr.UsedSlots())

		err = mgr.Release(context.Background(), leaseID)
		require.NoError(t, err)
		assert.Equal(t, int64(0), mgr.UsedSlots())
		assert.Equal(t, 100, mgr.AvailableSlots())
	})

	t.Run("acquire fails when no slots available", func(t *testing.T) {
		mgr := &SlotBlockManager{
			totalSlots:  1,
			ownedBlocks: map[int]bool{0: true},
			stopC:       make(chan struct{}),
			metrics:     &noOpSlotBlockMetrics{},
		}

		// First acquire succeeds
		_, err := mgr.Acquire(context.Background())
		require.NoError(t, err)

		// Second acquire fails
		_, err = mgr.Acquire(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "all 1 slots in use")
	})

	t.Run("acquire fails when no blocks owned", func(t *testing.T) {
		mgr := &SlotBlockManager{
			totalSlots:  0,
			ownedBlocks: map[int]bool{},
			stopC:       make(chan struct{}),
			metrics:     &noOpSlotBlockMetrics{},
		}

		_, err := mgr.Acquire(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no slot blocks acquired")
	})

	t.Run("release with empty lease ID is no-op", func(t *testing.T) {
		mgr := &SlotBlockManager{
			totalSlots:  100,
			ownedBlocks: map[int]bool{0: true},
			stopC:       make(chan struct{}),
			metrics:     &noOpSlotBlockMetrics{},
		}

		err := mgr.Release(context.Background(), "")
		require.NoError(t, err)
		assert.Equal(t, int64(0), mgr.UsedSlots())
	})
}

func TestSlotBlockManager_BlockPK(t *testing.T) {
	mgr := &SlotBlockManager{
		endpoint: "test-cluster.dsql.us-east-1.on.aws",
	}

	pk := mgr.blockPK(0)
	assert.Equal(t, "connslots#test-cluster.dsql.us-east-1.on.aws#block-0", pk)

	pk = mgr.blockPK(99)
	assert.Equal(t, "connslots#test-cluster.dsql.us-east-1.on.aws#block-99", pk)
}

func TestSlotBlockConfig_Defaults(t *testing.T) {
	cfg := DefaultSlotBlockConfig()

	assert.Equal(t, 100, cfg.BlockSize)
	assert.Equal(t, 100, cfg.BlockCount)
	assert.Equal(t, 3*time.Minute, cfg.TTL)
	assert.Equal(t, 1*time.Minute, cfg.RenewPeriod)
}

func TestGenerateOwnerID(t *testing.T) {
	id1, err := generateOwnerID()
	require.NoError(t, err)
	assert.Len(t, id1, 32) // 16 bytes = 32 hex chars

	id2, err := generateOwnerID()
	require.NoError(t, err)
	assert.NotEqual(t, id1, id2) // Should be unique
}
