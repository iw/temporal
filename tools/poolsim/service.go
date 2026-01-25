package main

import (
	"time"
)

// Service represents a Temporal service with its pool, warmup, keeper, and workload.
type Service struct {
	Name   string
	Config ServiceConfig

	Pool     *Pool
	Warmup   *Warmup
	Keeper   *Keeper
	Workload *Workload
}

// NewService creates a new service with all components.
func NewService(
	cfg ServiceConfig,
	endpoint string,
	limiter RateLimiter,
	metrics *Metrics,
	sim *Sim,
) *Service {
	pool := NewPool(cfg.Name, cfg.MaxOpen, cfg.MaxConnLifetime)

	warmup := &Warmup{
		ServiceName:  cfg.Name,
		Endpoint:     endpoint,
		Pool:         pool,
		Limiter:      limiter,
		Metrics:      metrics,
		TargetConns:  cfg.TargetOpen,
		StaggerDur:   cfg.WarmupStagger,
		MaxRetries:   5,
		RetryBackoff: 200 * time.Millisecond,
		Sim:          sim,
	}

	keeper := &Keeper{
		ServiceName:     cfg.Name,
		Endpoint:        endpoint,
		Pool:            pool,
		Limiter:         limiter,
		Metrics:         metrics,
		Target:          cfg.TargetOpen,
		MaxConnsPerTick: cfg.MaxConnsPerTick,
		TickInterval:    cfg.KeeperTick,
		Sim:             sim,
	}

	workload := &Workload{
		ServiceName: cfg.Name,
		Endpoint:    endpoint,
		Cfg:         cfg.Workload,
		Pool:        pool,
		Limiter:     limiter,
		Metrics:     metrics,
		Sim:         sim,
	}

	return &Service{
		Name:     cfg.Name,
		Config:   cfg,
		Pool:     pool,
		Warmup:   warmup,
		Keeper:   keeper,
		Workload: workload,
	}
}

// Start begins the service (warmup, then keeper, then workload).
func (s *Service) Start(at time.Time) {
	// Start warmup immediately
	s.Warmup.Start(at)

	// Start keeper after warmup would complete
	keeperStart := at.Add(s.Config.WarmupStagger + 1*time.Second)
	s.Keeper.Start(keeperStart)

	// Start workload after warmup (if enabled)
	if s.Config.Workload.Enabled {
		workloadStart := at.Add(s.Config.WarmupStagger + 2*time.Second)
		s.Workload.Start(workloadStart)
	}
}
