package main

import (
	"time"
)

// Warmup models the pool warmup behavior from pool_warmup.go.
// It creates connections with staggered delays to:
// 1. Respect the DSQL rate limit during startup
// 2. Spread connection ages so they don't all expire at once
type Warmup struct {
	ServiceName string
	Endpoint    string

	Pool    *Pool
	Limiter RateLimiter
	Metrics *Metrics

	TargetConns  int
	StaggerDur   time.Duration
	MaxRetries   int
	RetryBackoff time.Duration

	Sim *Sim

	// Stats
	created int
	retries int
}

// Start begins the warmup process.
func (w *Warmup) Start(at time.Time) {
	w.Sim.Schedule(at, w.ServiceName+":warmup_start", func() {
		w.startWarmup()
	})
}

func (w *Warmup) startWarmup() {
	now := w.Sim.Now()
	stats := w.Pool.Stats()

	needed := w.TargetConns - stats.Open
	if needed <= 0 {
		return
	}

	// Calculate delay between connections to spread across StaggerDur
	var delayBetween time.Duration
	if w.StaggerDur > 0 && needed > 1 {
		delayBetween = w.StaggerDur / time.Duration(needed-1)
	}

	// Schedule connection creation events
	for i := 0; i < needed; i++ {
		delay := time.Duration(i) * delayBetween
		idx := i
		w.Sim.Schedule(now.Add(delay), w.ServiceName+":warmup_conn", func() {
			w.createOneConnection(idx)
		})
	}
}

func (w *Warmup) createOneConnection(idx int) {
	now := w.Sim.Now()

	// Try to acquire rate limit permit
	ok, retryAfter := w.Limiter.Acquire(now, w.Endpoint, 1)
	if !ok {
		w.Metrics.RecordLimiterDeny(now, w.ServiceName)
		// Retry after rate limit window
		w.Sim.After(retryAfter, w.ServiceName+":warmup_retry", func() {
			w.createOneConnection(idx)
		})
		w.retries++
		return
	}

	// Create the connection
	if err := w.Pool.OpenNewConn(now); err != nil {
		// Pool full - this shouldn't happen during warmup
		return
	}

	w.created++
	w.Metrics.RecordWarmupOpen(now, w.ServiceName)
}

// Stats returns warmup statistics.
func (w *Warmup) Stats() (created, retries int) {
	return w.created, w.retries
}
