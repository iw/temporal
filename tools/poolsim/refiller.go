package main

import (
	"math/rand"
	"time"
)

// Refiller constants matching reservoir_refiller.go
const (
	// IdleCheckInterval is how often we check when at target capacity.
	// In production this is 100ms, but for simulation we use 1s to reduce event count.
	// This is safe because the expiry scanner runs every 1s and will trigger refills.
	IdleCheckInterval = 1 * time.Second

	// FailureBackoff is the backoff after a connection creation failure.
	FailureBackoff = 250 * time.Millisecond

	// ExpiryScanInterval is how often we scan for expired connections.
	ExpiryScanInterval = 1 * time.Second
)

// Refiller models the continuous refiller from reservoir_refiller.go.
// It runs back-to-back connection creation with rate limiter as the ONLY throttle.
type Refiller struct {
	ServiceName string
	Endpoint    string

	Reservoir *Reservoir
	Limiter   RateLimiter
	Metrics   *Metrics

	Sim *Sim
	rng *rand.Rand

	// Stats
	created int
	retries int
}

// Start begins the refiller loop and expiry scanner.
func (r *Refiller) Start(at time.Time) {
	if r.rng == nil {
		r.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	// Start refiller loop
	r.Sim.Schedule(at, r.ServiceName+":refiller_start", func() {
		r.loop()
	})

	// Start expiry scanner
	r.Sim.Schedule(at, r.ServiceName+":scanner_start", func() {
		r.scanLoop()
	})
}

// loop is the main refiller loop.
// Runs back-to-back openOne() calls - rate limiter is the ONLY throttle.
func (r *Refiller) loop() {
	now := r.Sim.Now()

	size := r.Reservoir.Size()
	need := r.Reservoir.Target - size

	if need <= 0 {
		// At target - brief check interval
		r.Sim.After(IdleCheckInterval, r.ServiceName+":refiller_idle", func() {
			r.loop()
		})
		return
	}

	// Try to create one connection
	success, shouldContinue := r.openOne(now)

	if !shouldContinue {
		// openOne already scheduled retry (rate limited)
		return
	}

	if !success {
		// Failed for other reason - backoff before retry
		r.Sim.After(FailureBackoff, r.ServiceName+":refiller_backoff", func() {
			r.loop()
		})
		r.retries++
		return
	}

	// Success - immediately try next (no delay)
	// Rate limiter is the only throttle
	r.Sim.Schedule(now, r.ServiceName+":refiller_next", func() {
		r.loop()
	})
}

// openOne attempts to create one connection.
// Returns (success, shouldContinue) - if shouldContinue is false, caller should not schedule next.
func (r *Refiller) openOne(now time.Time) (success bool, shouldContinue bool) {
	// Rate limit connection creation
	ok, retryAfter := r.Limiter.Acquire(now, r.Endpoint, 1)
	if !ok {
		r.Metrics.RecordLimiterDeny(now, r.ServiceName)
		r.Reservoir.RecordRefillFailure()

		// Schedule retry after rate limit window
		r.Sim.After(retryAfter, r.ServiceName+":refiller_ratelimit", func() {
			r.loop()
		})
		return false, false // Don't continue - we already scheduled retry
	}

	// Create connection with jittered lifetime
	jitterFunc := func() time.Duration {
		if r.Reservoir.Jitter <= 0 {
			return 0
		}
		return time.Duration(r.rng.Int63n(int64(r.Reservoir.Jitter)))
	}

	conn := r.Reservoir.CreateConn(now, jitterFunc)

	// Add to reservoir
	if !r.Reservoir.AddNew(conn) {
		// Reservoir full - shouldn't happen if we check need first
		return true, true // Still "successful" - just discarded
	}

	r.created++
	r.Metrics.RecordRefillerOpen(now, r.ServiceName)
	return true, true
}

// scanLoop runs the expiry scanner every ExpiryScanInterval.
func (r *Refiller) scanLoop() {
	now := r.Sim.Now()

	evicted := r.Reservoir.ScanAndEvict(now)
	if evicted > 0 {
		r.Metrics.RecordScanEvictions(now, r.ServiceName, evicted)
	}

	// Schedule next scan
	r.Sim.After(ExpiryScanInterval, r.ServiceName+":scanner_tick", func() {
		r.scanLoop()
	})
}

// Stats returns refiller statistics.
func (r *Refiller) Stats() (created, retries int) {
	return r.created, r.retries
}
