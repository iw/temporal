package main

import (
	"time"
)

// Service represents a Temporal service with its reservoir, refiller, and workload.
type Service struct {
	Name   string
	Config ServiceConfig

	Reservoir *Reservoir
	Refiller  *Refiller
	Workload  *ReservoirWorkload
}

// NewService creates a new service with all components.
func NewService(
	cfg ServiceConfig,
	endpoint string,
	limiter RateLimiter,
	metrics *Metrics,
	sim *Sim,
) *Service {
	reservoir := NewReservoir(
		cfg.Name,
		cfg.TargetReady,
		cfg.GuardWindow,
		cfg.BaseLifetime,
		cfg.Jitter,
	)

	refiller := &Refiller{
		ServiceName: cfg.Name,
		Endpoint:    endpoint,
		Reservoir:   reservoir,
		Limiter:     limiter,
		Metrics:     metrics,
		Sim:         sim,
	}

	workload := &ReservoirWorkload{
		ServiceName: cfg.Name,
		Endpoint:    endpoint,
		Cfg:         cfg.Workload,
		Reservoir:   reservoir,
		Metrics:     metrics,
		Sim:         sim,
	}

	return &Service{
		Name:      cfg.Name,
		Config:    cfg,
		Reservoir: reservoir,
		Refiller:  refiller,
		Workload:  workload,
	}
}

// Start begins the service (refiller + expiry scanner, then workload).
func (s *Service) Start(at time.Time) {
	// Start refiller and expiry scanner immediately
	s.Refiller.Start(at)

	// Start workload after initial fill would complete (if enabled)
	// Estimate: target / rate_limit seconds
	if s.Config.Workload.Enabled {
		// Give time for initial fill - estimate based on target and typical rate
		initialFillTime := time.Duration(s.Config.TargetReady) * 20 * time.Millisecond // ~50/sec
		workloadStart := at.Add(initialFillTime + 1*time.Second)
		s.Workload.Start(workloadStart)
	}
}
