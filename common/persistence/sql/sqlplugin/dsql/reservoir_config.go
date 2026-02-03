package dsql

import (
	"os"
	"strconv"
	"time"
)

// Reservoir configuration is currently driven by environment variables.
// This keeps the change isolated to the DSQL plugin while the design stabilizes.
//
// Note: getEnvInt and getEnvDuration helper functions are defined in
// connection_rate_limiter.go to avoid duplicate declarations.

const (
	ReservoirEnabledEnvVar            = "DSQL_RESERVOIR_ENABLED"
	ReservoirTargetReadyEnvVar        = "DSQL_RESERVOIR_TARGET_READY"
	ReservoirLowWatermarkEnvVar       = "DSQL_RESERVOIR_LOW_WATERMARK"
	ReservoirBaseLifetimeEnvVar       = "DSQL_RESERVOIR_BASE_LIFETIME"
	ReservoirLifetimeJitterEnvVar     = "DSQL_RESERVOIR_LIFETIME_JITTER"
	ReservoirGuardWindowEnvVar        = "DSQL_RESERVOIR_GUARD_WINDOW"
	ReservoirInitialFillTimeoutEnvVar = "DSQL_RESERVOIR_INITIAL_FILL_TIMEOUT"
	ReservoirInflightLimitEnvVar      = "DSQL_RESERVOIR_INFLIGHT_LIMIT"

	DefaultReservoirBaseLifetime       = 11 * time.Minute
	DefaultReservoirLifetimeJitter     = 2 * time.Minute
	DefaultReservoirGuardWindow        = 45 * time.Second
	DefaultReservoirInitialFillTimeout = 30 * time.Second
	DefaultReservoirInflightLimit      = 8

	// EphemeralPoolSize is the reservoir size for short-lived pools used during
	// startup (schema version checks, metadata initialization, namespace setup).
	// These pools are closed immediately after use, so a small size is sufficient.
	EphemeralPoolSize = 5
)

// PoolSizeHint values for config.SQL.PoolSizeHint
const (
	PoolSizeHintEphemeral = "ephemeral" // Short-lived pool for startup operations
	PoolSizeHintService   = "service"   // Long-lived pool for service operations (default)
)

type ReservoirConfig struct {
	Enabled            bool
	TargetReady        int
	LowWatermark       int
	BaseLifetime       time.Duration
	Jitter             time.Duration
	GuardWindow        time.Duration
	InitialFillTimeout time.Duration
	InflightLimit      int // Max concurrent Open() calls in refiller (default: 8)
}

func IsReservoirEnabled() bool {
	v := os.Getenv(ReservoirEnabledEnvVar)
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

// GetReservoirConfig returns the effective reservoir configuration.
// maxOpen is used to derive sensible defaults.
// poolSizeHint can be "ephemeral" for short-lived pools or "" / "service" for long-lived pools.
func GetReservoirConfig(maxOpen int, poolSizeHint string) ReservoirConfig {
	cfg := ReservoirConfig{Enabled: IsReservoirEnabled()}

	// For ephemeral pools, use a small fixed size regardless of env vars
	if poolSizeHint == PoolSizeHintEphemeral {
		cfg.TargetReady = EphemeralPoolSize
		cfg.LowWatermark = EphemeralPoolSize
		cfg.BaseLifetime = getEnvDuration(ReservoirBaseLifetimeEnvVar, DefaultReservoirBaseLifetime)
		cfg.Jitter = getEnvDuration(ReservoirLifetimeJitterEnvVar, DefaultReservoirLifetimeJitter)
		cfg.GuardWindow = getEnvDuration(ReservoirGuardWindowEnvVar, DefaultReservoirGuardWindow)
		cfg.InitialFillTimeout = 10 * time.Second // Short timeout for ephemeral pools
		cfg.InflightLimit = 2                     // Minimal concurrency for ephemeral pools
		return cfg
	}

	// Service pools use full configuration from env vars
	cfg.TargetReady = getEnvInt(ReservoirTargetReadyEnvVar, maxOpen)
	cfg.LowWatermark = getEnvInt(ReservoirLowWatermarkEnvVar, maxOpen)
	if cfg.TargetReady < cfg.LowWatermark {
		cfg.TargetReady = cfg.LowWatermark
	}

	cfg.BaseLifetime = getEnvDuration(ReservoirBaseLifetimeEnvVar, DefaultReservoirBaseLifetime)
	cfg.Jitter = getEnvDuration(ReservoirLifetimeJitterEnvVar, DefaultReservoirLifetimeJitter)
	cfg.GuardWindow = getEnvDuration(ReservoirGuardWindowEnvVar, DefaultReservoirGuardWindow)
	cfg.InitialFillTimeout = getEnvDuration(ReservoirInitialFillTimeoutEnvVar, DefaultReservoirInitialFillTimeout)
	cfg.InflightLimit = getEnvInt(ReservoirInflightLimitEnvVar, DefaultReservoirInflightLimit)
	if cfg.BaseLifetime <= 0 {
		cfg.BaseLifetime = DefaultReservoirBaseLifetime
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}
	if cfg.GuardWindow < 0 {
		cfg.GuardWindow = 0
	}
	if cfg.InitialFillTimeout < 0 {
		cfg.InitialFillTimeout = DefaultReservoirInitialFillTimeout
	}
	if cfg.InflightLimit <= 0 {
		cfg.InflightLimit = DefaultReservoirInflightLimit
	}
	return cfg
}
