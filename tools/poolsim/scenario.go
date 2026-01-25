package main

import (
	"fmt"
	"time"
)

// ApplyScenario configures the simulation scenario.
func ApplyScenario(cfg *Config, sim *Sim, services map[string]*Service) error {
	switch cfg.Scenario.Kind {
	case "cold_start":
		// Nothing extra - services just start and warm up
		return nil

	case "rolling_restart":
		// Restart services one-by-one
		rollEvery := cfg.Scenario.RollEvery
		if rollEvery <= 0 {
			rollEvery = 60 * time.Second
		}

		names := make([]string, 0, len(services))
		for name := range services {
			names = append(names, name)
		}

		// Schedule restarts starting after initial warmup period
		startAt := sim.Now().Add(5 * time.Minute) // Give time for initial warmup
		for i := 0; ; i++ {
			restartAt := startAt.Add(time.Duration(i) * rollEvery)
			if restartAt.After(sim.End()) {
				break
			}

			name := names[i%len(names)]
			svc := services[name]
			sim.Schedule(restartAt, "rolling_restart:"+name, func() {
				svc.Pool.DropAll()
			})
		}
		return nil

	case "mass_drop":
		// Drop a fraction of connections across all services
		dropAt := sim.Now().Add(cfg.Scenario.DropAt)
		frac := cfg.Scenario.DropFraction
		if frac <= 0 || frac > 1 {
			frac = 0.3
		}

		sim.Schedule(dropAt, "mass_drop", func() {
			for _, svc := range services {
				svc.Pool.DropFraction(frac)
			}
		})
		return nil

	default:
		return fmt.Errorf("unknown scenario kind: %q", cfg.Scenario.Kind)
	}
}
