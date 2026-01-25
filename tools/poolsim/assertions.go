package main

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// AssertConnectRate verifies the global connection rate never exceeds the limit.
func AssertConnectRate(cfg *Config, m *Metrics) error {
	limit := cfg.AssertMaxConnectsPerSec
	if limit <= 0 {
		return nil
	}
	for sec, n := range m.GlobalConnectsPerSecond() {
		if n > limit {
			return fmt.Errorf("global connects/sec exceeded: sec=%d connects=%d limit=%d", sec, n, limit)
		}
	}
	return nil
}

// AssertConverge verifies each service reaches target within the deadline
// and optionally stays stable for a duration.
func AssertConverge(cfg *Config, m *Metrics) error {
	within := cfg.AssertConvergeWithin
	stableFor := cfg.AssertStableFor
	if within <= 0 {
		return nil
	}
	if stableFor < 0 {
		stableFor = 0
	}

	// Build target map from config
	target := make(map[string]int)
	startDelay := make(map[string]time.Duration)
	for _, s := range cfg.Services {
		target[s.Name] = s.TargetOpen
		startDelay[s.Name] = s.StartDelay
	}

	// Group samples by service
	bySvc := make(map[string][]Sample)
	for _, s := range m.Samples() {
		bySvc[s.Service] = append(bySvc[s.Service], s)
	}

	for svc, samples := range bySvc {
		if len(samples) == 0 {
			return fmt.Errorf("no samples for service %s", svc)
		}

		// Sort by time
		sort.Slice(samples, func(i, j int) bool {
			return samples[i].Time.Before(samples[j].Time)
		})

		// Calculate deadline relative to service start
		t0 := samples[0].Time.Add(startDelay[svc])
		deadline := t0.Add(within)
		tgt := target[svc]
		if tgt == 0 {
			continue
		}

		// Find when service first reached target
		var reachedAt *time.Time
		for i := range samples {
			if samples[i].Time.Before(t0) {
				continue
			}
			if samples[i].Open >= tgt {
				t := samples[i].Time
				reachedAt = &t
				break
			}
		}

		if reachedAt == nil {
			last := samples[len(samples)-1]
			return fmt.Errorf("service %s never reached target open=%d (last open=%d at %s)",
				svc, tgt, last.Open, last.Time.Format(time.RFC3339))
		}

		if reachedAt.After(deadline) {
			return fmt.Errorf("service %s reached target too late: reachedAt=%s deadline=%s",
				svc, reachedAt.Format(time.RFC3339), deadline.Format(time.RFC3339))
		}

		// Check stability if required
		if stableFor > 0 {
			stableUntil := reachedAt.Add(stableFor)
			for i := range samples {
				if samples[i].Time.Before(*reachedAt) {
					continue
				}
				if samples[i].Time.After(stableUntil) {
					break
				}
				if samples[i].Open < tgt {
					return fmt.Errorf("service %s not stable at target: dropped below %d at %s (open=%d)",
						svc, tgt, samples[i].Time.Format(time.RFC3339), samples[i].Open)
				}
			}
		}
	}
	return nil
}

// AssertClosureSpike detects thundering herd by checking closure rate.
func AssertClosureSpike(cfg *Config, m *Metrics) error {
	limit := cfg.AssertMaxClosurePerSec
	if limit <= 0 {
		return nil
	}

	type lastKey struct{ svc string }
	last := make(map[lastKey]Sample)
	closuresPerSec := make(map[int64]float64)

	samples := m.Samples()
	for _, s := range samples {
		k := lastKey{svc: s.Service}
		if prev, ok := last[k]; ok {
			dLife := float64(s.ClosedLifetime - prev.ClosedLifetime)
			dSrv := float64(s.ClosedServer - prev.ClosedServer)
			d := dLife + dSrv
			if d < 0 {
				d = 0
			}
			closuresPerSec[s.Time.Unix()] += d
		}
		last[k] = s
	}

	for sec, c := range closuresPerSec {
		if int(math.Ceil(c)) > limit {
			return fmt.Errorf("closure spike exceeded: sec=%d closures=%.2f limit=%d", sec, c, limit)
		}
	}
	return nil
}
