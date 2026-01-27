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

	DefaultReservoirBaseLifetime       = 11 * time.Minute
	DefaultReservoirLifetimeJitter     = 2 * time.Minute
	DefaultReservoirGuardWindow        = 45 * time.Second
	DefaultReservoirInitialFillTimeout = 30 * time.Second
)

type ReservoirConfig struct {
	Enabled            bool
	TargetReady        int
	LowWatermark       int
	BaseLifetime       time.Duration
	Jitter             time.Duration
	GuardWindow        time.Duration
	InitialFillTimeout time.Duration
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
func GetReservoirConfig(maxOpen int) ReservoirConfig {
	cfg := ReservoirConfig{Enabled: IsReservoirEnabled()}

	cfg.TargetReady = getEnvInt(ReservoirTargetReadyEnvVar, maxOpen)
	cfg.LowWatermark = getEnvInt(ReservoirLowWatermarkEnvVar, maxOpen)
	if cfg.TargetReady < cfg.LowWatermark {
		cfg.TargetReady = cfg.LowWatermark
	}

	cfg.BaseLifetime = getEnvDuration(ReservoirBaseLifetimeEnvVar, DefaultReservoirBaseLifetime)
	cfg.Jitter = getEnvDuration(ReservoirLifetimeJitterEnvVar, DefaultReservoirLifetimeJitter)
	cfg.GuardWindow = getEnvDuration(ReservoirGuardWindowEnvVar, DefaultReservoirGuardWindow)
	cfg.InitialFillTimeout = getEnvDuration(ReservoirInitialFillTimeoutEnvVar, DefaultReservoirInitialFillTimeout)
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
	return cfg
}
