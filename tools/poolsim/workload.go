package main

import (
	"math/rand"
	"time"
)

// WorkloadConfig configures simulated database workload.
type WorkloadConfig struct {
	// Enabled controls whether workload simulation runs.
	Enabled bool

	// MaxInUse is the target number of concurrent connections in use.
	// Workload will try to maintain this many active "queries".
	MaxInUse int

	// HoldTime is how long each "query" holds a connection.
	HoldTime time.Duration

	// HoldJitter adds randomness to hold time (0 to HoldJitter added).
	HoldJitter time.Duration

	// SpawnEvery is how often the workload spawns new work.
	SpawnEvery time.Duration

	// ArrivalBurst is max work items to start per spawn tick.
	ArrivalBurst int

	// MaxWait is max time to wait for a connection before giving up.
	MaxWait time.Duration
}

// Workload simulates database queries consuming connections.
// This models Temporal's persistence layer making database calls.
type Workload struct {
	ServiceName string
	Endpoint    string

	Cfg WorkloadConfig

	Pool    *Pool
	Limiter RateLimiter
	Metrics *Metrics
	Sim     *Sim

	startedAt time.Time
}

// Start begins the workload simulation.
func (w *Workload) Start(at time.Time) {
	if !w.Cfg.Enabled {
		return
	}
	w.startedAt = at
	w.Sim.Schedule(at, w.ServiceName+":workload_start", func() {
		w.tick()
	})
}

func (w *Workload) tick() {
	now := w.Sim.Now()

	// How many connections are currently in use?
	stats := w.Pool.Stats()
	inUse := stats.InUse

	// How many more do we need to reach MaxInUse?
	need := w.Cfg.MaxInUse - inUse
	if need <= 0 {
		// Already at target, schedule next tick
		w.Sim.After(w.Cfg.SpawnEvery, w.ServiceName+":workload_tick", func() {
			w.tick()
		})
		return
	}

	// Start up to ArrivalBurst work items
	toStart := need
	if toStart > w.Cfg.ArrivalBurst {
		toStart = w.Cfg.ArrivalBurst
	}

	for i := 0; i < toStart; i++ {
		w.tryStartOne(now)
	}

	w.Sim.After(w.Cfg.SpawnEvery, w.ServiceName+":workload_tick", func() {
		w.tick()
	})
}

func (w *Workload) tryStartOne(now time.Time) {
	br := w.Pool.Borrow(now, w.Limiter, w.Endpoint)

	if br.WaitFor > 0 {
		// Need to wait - record and retry
		w.Metrics.RecordPoolWait(now, w.ServiceName, br.WaitFor, br.DeniedByLimiter)

		// Retry if within MaxWait
		if w.Cfg.MaxWait > 0 && now.Sub(w.startedAt) < w.Cfg.MaxWait {
			w.Sim.After(br.WaitFor, w.ServiceName+":work_retry", func() {
				w.tryStartOne(w.Sim.Now())
			})
		}
		return
	}

	// Got a connection - record if we opened a new one
	if br.Opened {
		w.Metrics.RecordWorkloadOpen(now, w.ServiceName)
	}

	w.Metrics.RecordWorkStarted(now, w.ServiceName)

	// Calculate hold time with jitter
	hold := w.Cfg.HoldTime
	if w.Cfg.HoldJitter > 0 {
		hold += time.Duration(rand.Int63n(int64(w.Cfg.HoldJitter + 1)))
	}

	// Schedule release
	connID := br.ConnID
	w.Sim.After(hold, w.ServiceName+":work_release", func() {
		w.Pool.Release(connID)
		w.Metrics.RecordWorkCompleted(w.Sim.Now(), w.ServiceName)
	})
}
