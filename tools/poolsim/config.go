package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the simulation configuration.
type Config struct {
	Seed int64 `json:"seed" yaml:"seed"`

	Endpoint string `json:"endpoint" yaml:"endpoint"`

	Duration    time.Duration `json:"duration" yaml:"duration"`
	SampleEvery time.Duration `json:"sampleEvery" yaml:"sampleEvery"`

	LimiterPerSec int `json:"limiterPerSec" yaml:"limiterPerSec"`

	// Assertions
	AssertMaxConnectsPerSec int           `json:"assertMaxConnectsPerSec" yaml:"assertMaxConnectsPerSec"`
	AssertConvergeWithin    time.Duration `json:"assertConvergeWithin" yaml:"assertConvergeWithin"`
	AssertStableFor         time.Duration `json:"assertStableFor" yaml:"assertStableFor"`
	AssertMaxClosurePerSec  int           `json:"assertMaxClosurePerSec" yaml:"assertMaxClosurePerSec"`

	Services []ServiceConfig `json:"services" yaml:"services"`
	Scenario ScenarioConfig  `json:"scenario" yaml:"scenario"`
}

// ServiceConfig configures a single service's pool behavior.
type ServiceConfig struct {
	Name string `json:"name" yaml:"name"`

	// Pool configuration
	TargetOpen int `json:"targetOpen" yaml:"targetOpen"`
	MaxOpen    int `json:"maxOpen" yaml:"maxOpen"`

	// Connection lifecycle (matches session.go defaults)
	MaxConnLifetime time.Duration `json:"maxConnLifetime" yaml:"maxConnLifetime"`

	// Warmup configuration (matches pool_warmup.go)
	WarmupStagger time.Duration `json:"warmupStagger" yaml:"warmupStagger"`

	// Keeper configuration (matches pool_keeper.go)
	KeeperTick      time.Duration `json:"keeperTick" yaml:"keeperTick"`
	MaxConnsPerTick int           `json:"maxConnsPerTick" yaml:"maxConnsPerTick"`

	// Startup delay (staggered service startup)
	StartDelay time.Duration `json:"startDelay" yaml:"startDelay"`

	// Workload simulation
	Workload WorkloadConfig `json:"workload" yaml:"workload"`
}

// ScenarioConfig configures the simulation scenario.
type ScenarioConfig struct {
	Kind string `json:"kind" yaml:"kind"` // cold_start | rolling_restart | mass_drop

	// Rolling restart
	RollEvery time.Duration `json:"rollEvery" yaml:"rollEvery"`

	// Mass drop
	DropAt       time.Duration `json:"dropAt" yaml:"dropAt"`
	DropFraction float64       `json:"dropFraction" yaml:"dropFraction"`
}

// LoadConfig loads configuration from a file.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	cfg := new(Config)
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("yaml unmarshal: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("json unmarshal: %w", err)
		}
	default:
		// Try YAML first, then JSON
		if err := yaml.Unmarshal(b, cfg); err != nil {
			if err := json.Unmarshal(b, cfg); err != nil {
				return nil, fmt.Errorf("unknown config format")
			}
		}
	}

	applyDefaults(cfg)
	return cfg, validate(cfg)
}

func applyDefaults(cfg *Config) {
	if cfg.Seed == 0 {
		cfg.Seed = 42
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "cluster.dsql.eu-west-1.on.aws"
	}
	if cfg.Duration == 0 {
		cfg.Duration = 20 * time.Minute
	}
	if cfg.SampleEvery == 0 {
		cfg.SampleEvery = 1 * time.Second
	}
	if cfg.LimiterPerSec == 0 {
		cfg.LimiterPerSec = 100 // DSQL cluster rate limit
	}

	if cfg.AssertMaxConnectsPerSec == 0 {
		cfg.AssertMaxConnectsPerSec = cfg.LimiterPerSec
	}
	if cfg.AssertConvergeWithin == 0 {
		cfg.AssertConvergeWithin = 60 * time.Second
	}
	if cfg.AssertStableFor == 0 {
		cfg.AssertStableFor = 5 * time.Second
	}
	if cfg.AssertMaxClosurePerSec == 0 {
		cfg.AssertMaxClosurePerSec = 200
	}

	if cfg.Scenario.Kind == "" {
		cfg.Scenario.Kind = "cold_start"
	}

	for i := range cfg.Services {
		s := &cfg.Services[i]

		if s.MaxOpen == 0 {
			s.MaxOpen = s.TargetOpen
		}

		// Default: session.DefaultMaxConnLifetime = 10 minutes
		if s.MaxConnLifetime == 0 {
			s.MaxConnLifetime = 10 * time.Minute
		}

		// Default: pool_warmup.go calculates based on pool size
		if s.WarmupStagger == 0 {
			s.WarmupStagger = calculateWarmupStagger(s.TargetOpen)
		}

		// Default: pool_keeper.go tick interval
		if s.KeeperTick == 0 {
			s.KeeperTick = 1 * time.Second
		}

		// Default: pool_keeper.go calculates based on pool size
		if s.MaxConnsPerTick == 0 {
			s.MaxConnsPerTick = calculateMaxConnsPerTick(s.TargetOpen)
		}

		// Workload defaults
		if s.Workload.SpawnEvery == 0 {
			s.Workload.SpawnEvery = 200 * time.Millisecond
		}
		if s.Workload.ArrivalBurst == 0 {
			s.Workload.ArrivalBurst = 5
		}
		if s.Workload.MaxWait == 0 {
			s.Workload.MaxWait = 10 * time.Second
		}
		if s.Workload.HoldTime == 0 {
			s.Workload.HoldTime = 10 * time.Millisecond
		}
	}
}

// calculateWarmupStagger matches pool_warmup.go CalculateStaggerDuration
func calculateWarmupStagger(poolSize int) time.Duration {
	if poolSize <= 100 {
		return 0
	}
	if poolSize <= 500 {
		return 2 * time.Minute
	}
	// Large pools: scale based on rate limit (50% of 100/sec = 50/sec)
	minSeconds := poolSize / 50
	minutes := (minSeconds / 60) + 1
	if minutes < 3 {
		minutes = 3
	}
	return time.Duration(minutes) * time.Minute
}

// calculateMaxConnsPerTick matches pool_keeper.go DefaultPoolKeeperConfig.
//
// This implements ratio-based batching: the batch size scales with pool size
// to handle peak expiry rates during connection lifecycle rotation.
//
// IMPORTANT: This is a LOCAL desire, not a guarantee. The global rate limiter
// (100 conn/sec cluster-wide) is the HARD constraint. If multiple pools
// compete for the rate limit budget, they back off and retry.
//
// Formula: MaxConnsPerTick = ceil(poolSize / staggerSeconds), capped at 10
//
// Examples:
//   - 50 connections:  ceil(50/120)  = 1 per tick
//   - 100 connections: ceil(100/120) = 1 per tick
//   - 500 connections: ceil(500/120) = 5 per tick
//   - 1000+ connections: capped at 10 per tick
//
// With a 2-minute (120s) stagger window and 1-second tick interval,
// this ensures the keeper can refill at the peak expiry rate when
// rate limit budget is available.
func calculateMaxConnsPerTick(poolSize int) int {
	// With 2-minute stagger and 1-second tick:
	// Peak expiry rate = poolSize / 120
	// MaxConnsPerTick = ceil(poolSize / 120), capped at 10
	staggerSeconds := 120
	maxConns := (poolSize + staggerSeconds - 1) / staggerSeconds
	if maxConns < 1 {
		maxConns = 1
	}
	if maxConns > 10 {
		maxConns = 10
	}
	return maxConns
}

func validate(cfg *Config) error {
	if len(cfg.Services) == 0 {
		return fmt.Errorf("config: services must be non-empty")
	}
	if cfg.LimiterPerSec <= 0 {
		return fmt.Errorf("config: limiterPerSec must be > 0")
	}

	for _, s := range cfg.Services {
		if s.Name == "" {
			return fmt.Errorf("config: each service needs a name")
		}
		if s.TargetOpen <= 0 || s.MaxOpen <= 0 {
			return fmt.Errorf("config: service %s requires targetOpen/maxOpen > 0", s.Name)
		}
		if s.TargetOpen > s.MaxOpen {
			return fmt.Errorf("config: service %s targetOpen > maxOpen", s.Name)
		}
	}

	switch cfg.Scenario.Kind {
	case "cold_start":
	case "rolling_restart":
		if cfg.Scenario.RollEvery == 0 {
			cfg.Scenario.RollEvery = 60 * time.Second
		}
	case "mass_drop":
		if cfg.Scenario.DropAt == 0 {
			cfg.Scenario.DropAt = 5 * time.Minute
		}
		if cfg.Scenario.DropFraction <= 0 || cfg.Scenario.DropFraction > 1 {
			cfg.Scenario.DropFraction = 0.3
		}
	default:
		return fmt.Errorf("config: unknown scenario.kind %q", cfg.Scenario.Kind)
	}

	return nil
}

// DefaultConfig returns a default configuration for local testing.
func DefaultConfig() *Config {
	cfg := &Config{
		Seed:                    42,
		Endpoint:                "cluster.dsql.eu-west-1.on.aws",
		Duration:                20 * time.Minute,
		SampleEvery:             1 * time.Second,
		LimiterPerSec:           100,
		AssertMaxConnectsPerSec: 100,
		AssertConvergeWithin:    60 * time.Second,
		AssertMaxClosurePerSec:  100,
		Scenario: ScenarioConfig{
			Kind: "cold_start",
		},
		Services: []ServiceConfig{
			{
				Name:            "history",
				TargetOpen:      50,
				MaxOpen:         50,
				MaxConnLifetime: 10 * time.Minute,
				WarmupStagger:   0,
				KeeperTick:      1 * time.Second,
				MaxConnsPerTick: 5,
				StartDelay:      0,
			},
			{
				Name:            "matching",
				TargetOpen:      50,
				MaxOpen:         50,
				MaxConnLifetime: 10 * time.Minute,
				WarmupStagger:   0,
				KeeperTick:      1 * time.Second,
				MaxConnsPerTick: 5,
				StartDelay:      100 * time.Millisecond,
			},
			{
				Name:            "frontend",
				TargetOpen:      50,
				MaxOpen:         50,
				MaxConnLifetime: 10 * time.Minute,
				WarmupStagger:   0,
				KeeperTick:      1 * time.Second,
				MaxConnsPerTick: 5,
				StartDelay:      200 * time.Millisecond,
			},
			{
				Name:            "worker",
				TargetOpen:      50,
				MaxOpen:         50,
				MaxConnLifetime: 10 * time.Minute,
				WarmupStagger:   0,
				KeeperTick:      1 * time.Second,
				MaxConnsPerTick: 5,
				StartDelay:      300 * time.Millisecond,
			},
		},
	}
	applyDefaults(cfg)
	return cfg
}
