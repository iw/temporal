package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"time"
)

// Sample represents a point-in-time snapshot of pool state.
type Sample struct {
	Time           time.Time
	Service        string
	Open           int
	Idle           int
	InUse          int
	Target         int
	ClosedLifetime int64
	ClosedServer   int64
	WarmupOpens    int64
	KeeperOpens    int64
	WorkloadOpens  int64
	LimiterDenies  int64
	PoolWaits      int64
	WorkStarted    int64
	WorkCompleted  int64
}

// Metrics collects simulation metrics.
type Metrics struct {
	warmupOpens   map[string]int64
	keeperOpens   map[string]int64
	workloadOpens map[string]int64
	limiterDenies map[string]int64
	poolWaits     map[string]int64
	workStarted   map[string]int64
	workCompleted map[string]int64

	// Per-second global connects for assertions
	globalConnects map[int64]int

	samples []Sample
}

// NewMetrics creates a new metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		warmupOpens:    make(map[string]int64),
		keeperOpens:    make(map[string]int64),
		workloadOpens:  make(map[string]int64),
		limiterDenies:  make(map[string]int64),
		poolWaits:      make(map[string]int64),
		workStarted:    make(map[string]int64),
		workCompleted:  make(map[string]int64),
		globalConnects: make(map[int64]int),
		samples:        make([]Sample, 0, 2048),
	}
}

// RecordWarmupOpen records a connection opened during warmup.
func (m *Metrics) RecordWarmupOpen(now time.Time, service string) {
	m.warmupOpens[service]++
	m.globalConnects[now.Unix()]++
}

// RecordKeeperOpen records a connection opened by the keeper.
func (m *Metrics) RecordKeeperOpen(now time.Time, service string) {
	m.keeperOpens[service]++
	m.globalConnects[now.Unix()]++
}

// RecordWorkloadOpen records a connection opened by workload demand.
func (m *Metrics) RecordWorkloadOpen(now time.Time, service string) {
	m.workloadOpens[service]++
	m.globalConnects[now.Unix()]++
}

// RecordLimiterDeny records a rate limit denial.
func (m *Metrics) RecordLimiterDeny(now time.Time, service string) {
	m.limiterDenies[service]++
}

// RecordPoolWait records when workload had to wait for a connection.
func (m *Metrics) RecordPoolWait(now time.Time, service string, waitDur time.Duration, deniedByLimiter bool) {
	m.poolWaits[service]++
	if deniedByLimiter {
		m.limiterDenies[service]++
	}
}

// RecordWorkStarted records a work item starting.
func (m *Metrics) RecordWorkStarted(now time.Time, service string) {
	m.workStarted[service]++
}

// RecordWorkCompleted records a work item completing.
func (m *Metrics) RecordWorkCompleted(now time.Time, service string) {
	m.workCompleted[service]++
}

// Snapshot captures current pool state.
func (m *Metrics) Snapshot(now time.Time, service string, pool *Pool, target int) {
	stats := pool.Stats()
	m.samples = append(m.samples, Sample{
		Time:           now,
		Service:        service,
		Open:           stats.Open,
		Idle:           stats.Idle,
		InUse:          stats.InUse,
		Target:         target,
		ClosedLifetime: stats.ClosedLifetime,
		ClosedServer:   stats.ClosedServer,
		WarmupOpens:    m.warmupOpens[service],
		KeeperOpens:    m.keeperOpens[service],
		WorkloadOpens:  m.workloadOpens[service],
		LimiterDenies:  m.limiterDenies[service],
		PoolWaits:      m.poolWaits[service],
		WorkStarted:    m.workStarted[service],
		WorkCompleted:  m.workCompleted[service],
	})
}

// WriteCSV writes metrics to a CSV file.
func (m *Metrics) WriteCSV(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"t_rfc3339", "service", "open", "idle", "in_use", "target",
		"closed_lifetime", "closed_server",
		"warmup_opens", "keeper_opens", "workload_opens",
		"limiter_denies", "pool_waits",
		"work_started", "work_completed",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, s := range m.samples {
		row := []string{
			s.Time.Format(time.RFC3339),
			s.Service,
			fmt.Sprintf("%d", s.Open),
			fmt.Sprintf("%d", s.Idle),
			fmt.Sprintf("%d", s.InUse),
			fmt.Sprintf("%d", s.Target),
			fmt.Sprintf("%d", s.ClosedLifetime),
			fmt.Sprintf("%d", s.ClosedServer),
			fmt.Sprintf("%d", s.WarmupOpens),
			fmt.Sprintf("%d", s.KeeperOpens),
			fmt.Sprintf("%d", s.WorkloadOpens),
			fmt.Sprintf("%d", s.LimiterDenies),
			fmt.Sprintf("%d", s.PoolWaits),
			fmt.Sprintf("%d", s.WorkStarted),
			fmt.Sprintf("%d", s.WorkCompleted),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// GlobalConnectsPerSecond returns per-second connection counts.
func (m *Metrics) GlobalConnectsPerSecond() map[int64]int {
	return m.globalConnects
}

// MaxConnectsPerSecond returns the maximum connections in any single second.
func (m *Metrics) MaxConnectsPerSecond() int {
	max := 0
	for _, count := range m.globalConnects {
		if count > max {
			max = count
		}
	}
	return max
}

// Samples returns a sorted copy of all samples.
func (m *Metrics) Samples() []Sample {
	out := make([]Sample, len(m.samples))
	copy(out, m.samples)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Time.Equal(out[j].Time) {
			return out[i].Service < out[j].Service
		}
		return out[i].Time.Before(out[j].Time)
	})
	return out
}
