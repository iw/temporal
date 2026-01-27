package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"time"
)

// Sample represents a point-in-time snapshot of reservoir state.
type Sample struct {
	Time    time.Time
	Service string

	// Reservoir state
	Size   int
	Target int
	InUse  int

	// Cumulative counters
	Checkouts      int64
	EmptyEvents    int64
	Discards       int64
	DiscardExpired int64
	DiscardGuard   int64
	DiscardFull    int64
	Refills        int64
	RefillFailures int64
	ScanEvictions  int64
	LimiterDenies  int64

	// Workload stats
	WorkStarted   int64
	WorkCompleted int64
}

// Metrics collects simulation metrics.
type Metrics struct {
	refillerOpens map[string]int64
	scanEvictions map[string]int64
	limiterDenies map[string]int64
	workStarted   map[string]int64
	workCompleted map[string]int64

	// Per-second global connects for assertions
	globalConnects map[int64]int

	samples []Sample
}

// NewMetrics creates a new metrics collector.
func NewMetrics() *Metrics {
	return &Metrics{
		refillerOpens:  make(map[string]int64),
		scanEvictions:  make(map[string]int64),
		limiterDenies:  make(map[string]int64),
		workStarted:    make(map[string]int64),
		workCompleted:  make(map[string]int64),
		globalConnects: make(map[int64]int),
		samples:        make([]Sample, 0, 2048),
	}
}

// RecordRefillerOpen records a connection created by the refiller.
func (m *Metrics) RecordRefillerOpen(now time.Time, service string) {
	m.refillerOpens[service]++
	m.globalConnects[now.Unix()]++
}

// RecordScanEvictions records connections evicted by the expiry scanner.
func (m *Metrics) RecordScanEvictions(now time.Time, service string, count int) {
	m.scanEvictions[service] += int64(count)
}

// RecordLimiterDeny records a rate limit denial.
func (m *Metrics) RecordLimiterDeny(now time.Time, service string) {
	m.limiterDenies[service]++
}

// RecordWorkStarted records a work item starting.
func (m *Metrics) RecordWorkStarted(now time.Time, service string) {
	m.workStarted[service]++
}

// RecordWorkCompleted records a work item completing.
func (m *Metrics) RecordWorkCompleted(now time.Time, service string) {
	m.workCompleted[service]++
}

// Snapshot captures current reservoir state.
func (m *Metrics) Snapshot(now time.Time, service string, reservoir *Reservoir) {
	stats := reservoir.Stats()
	m.samples = append(m.samples, Sample{
		Time:           now,
		Service:        service,
		Size:           stats.Size,
		Target:         stats.Target,
		InUse:          stats.InUse,
		Checkouts:      stats.Checkouts,
		EmptyEvents:    stats.EmptyEvents,
		Discards:       stats.Discards,
		DiscardExpired: stats.DiscardExpired,
		DiscardGuard:   stats.DiscardGuard,
		DiscardFull:    stats.DiscardFull,
		Refills:        stats.Refills,
		RefillFailures: stats.RefillFailures,
		ScanEvictions:  m.scanEvictions[service],
		LimiterDenies:  m.limiterDenies[service],
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
		"t_rfc3339", "service",
		"size", "target", "in_use",
		"checkouts", "empty_events",
		"discards", "discard_expired", "discard_guard", "discard_full",
		"refills", "refill_failures", "scan_evictions",
		"limiter_denies",
		"work_started", "work_completed",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, s := range m.samples {
		row := []string{
			s.Time.Format(time.RFC3339),
			s.Service,
			fmt.Sprintf("%d", s.Size),
			fmt.Sprintf("%d", s.Target),
			fmt.Sprintf("%d", s.InUse),
			fmt.Sprintf("%d", s.Checkouts),
			fmt.Sprintf("%d", s.EmptyEvents),
			fmt.Sprintf("%d", s.Discards),
			fmt.Sprintf("%d", s.DiscardExpired),
			fmt.Sprintf("%d", s.DiscardGuard),
			fmt.Sprintf("%d", s.DiscardFull),
			fmt.Sprintf("%d", s.Refills),
			fmt.Sprintf("%d", s.RefillFailures),
			fmt.Sprintf("%d", s.ScanEvictions),
			fmt.Sprintf("%d", s.LimiterDenies),
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

// TotalEmptyEvents returns total empty events across all services.
func (m *Metrics) TotalEmptyEvents() int64 {
	var total int64
	for _, s := range m.samples {
		if s.EmptyEvents > total {
			total = s.EmptyEvents
		}
	}
	return total
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
