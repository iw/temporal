package dsql

import (
	"os"
	"strconv"
)

const (
	DistributedConnLeaseEnabledEnvVar = "DSQL_DISTRIBUTED_CONN_LEASE_ENABLED"
	DistributedConnLeaseTableEnvVar   = "DSQL_DISTRIBUTED_CONN_LEASE_TABLE"
	DistributedConnLimitEnvVar        = "DSQL_DISTRIBUTED_CONN_LIMIT"

	DefaultDistributedConnLimit = 10000
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
