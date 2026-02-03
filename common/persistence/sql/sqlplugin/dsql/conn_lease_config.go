package dsql

import (
	"os"
	"strconv"
	"time"
)

const (
	DistributedConnLeaseEnabledEnvVar = "DSQL_DISTRIBUTED_CONN_LEASE_ENABLED"
	DistributedConnLeaseTableEnvVar   = "DSQL_DISTRIBUTED_CONN_LEASE_TABLE"
	DistributedConnLimitEnvVar        = "DSQL_DISTRIBUTED_CONN_LIMIT"

	// Slot block configuration
	SlotBlockSizeEnvVar  = "DSQL_SLOT_BLOCK_SIZE"
	SlotBlockCountEnvVar = "DSQL_SLOT_BLOCK_COUNT"
	SlotBlockTTLEnvVar   = "DSQL_SLOT_BLOCK_TTL"
	SlotBlockRenewEnvVar = "DSQL_SLOT_BLOCK_RENEW_INTERVAL"

	DefaultDistributedConnLimit = 10000
	DefaultSlotBlockSize        = 100
	DefaultSlotBlockCount       = 100 // 100 blocks × 100 slots = 10,000 total
	DefaultSlotBlockTTL         = 3 * time.Minute
	DefaultSlotBlockRenew       = 1 * time.Minute
)

func IsDistributedConnLeaseEnabled() bool {
	v := os.Getenv(DistributedConnLeaseEnabledEnvVar)
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

func GetDistributedConnLeaseTable() string {
	return os.Getenv(DistributedConnLeaseTableEnvVar)
}

func GetDistributedConnLimit() int64 {
	v := os.Getenv(DistributedConnLimitEnvVar)
	if v == "" {
		return DefaultDistributedConnLimit
	}
	i, err := strconv.ParseInt(v, 10, 64)
	if err != nil || i <= 0 {
		return DefaultDistributedConnLimit
	}
	return i
}

// GetSlotBlockConfig returns the slot block configuration.
// targetPoolSize is used to calculate how many blocks a service might need.
func GetSlotBlockConfig(targetPoolSize int) SlotBlockConfig {
	blockSize := getEnvInt(SlotBlockSizeEnvVar, DefaultSlotBlockSize)
	blockCount := getEnvInt(SlotBlockCountEnvVar, DefaultSlotBlockCount)
	ttl := getEnvDuration(SlotBlockTTLEnvVar, DefaultSlotBlockTTL)
	renewInterval := getEnvDuration(SlotBlockRenewEnvVar, DefaultSlotBlockRenew)

	return SlotBlockConfig{
		BlockSize:   blockSize,
		BlockCount:  blockCount,
		TTL:         ttl,
		RenewPeriod: renewInterval,
	}
}
