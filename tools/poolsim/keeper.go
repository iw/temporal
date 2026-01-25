package main

import (
	"time"
)

// Keeper models the Pool Keeper behavior from pool_keeper.go.
// It runs on a tick interval and tops up the pool when connections
// are closed (by MaxConnLifetime or server events).
//
// Key behaviors:
//   - Edge-triggered: only acts when open < target
//   - Ratio-based batching: creates at most MaxConnsPerTick per tick
//     (calculated as ceil(poolSize/120), capped at 10)
//   - Respects global rate limiter
//
// # Batch Size vs Global Rate Limit
//
// The per-pool batch size (MaxConnsPerTick) is a LOCAL desire, not a guarantee.
// The global rate limiter (100 conn/sec cluster-wide) is the HARD constraint.
//
// With 44 pools × 10 max/tick = 440 potential connections/second, but only
// 100 are allowed. The rate limiter enforces this - when a pool hits the
// limit mid-batch, it backs off and retries next second.
//
// Why this works in steady state:
//   - Warmup stagger creates implicit jitter in connection ages
//   - Connections don't all expire at the same time
//   - Typical steady-state demand is 1-2 connections/tick per pool
//   - The batch size (10) is headroom for burst recovery, not normal operation
//
// The simulation validates that even with this contention, pools converge
// and stay stable because the stagger distributes expiry over time.
type Keeper struct {
	ServiceName string
	Endpoint    string

	Pool    *Pool
	Limiter RateLimiter
	Metrics *Metrics

	Target          int
	MaxConnsPerTick int
	TickInterval    time.Duration

	Sim *Sim

	// Stats
	topups  int
	retries int
}

// Start begins the keeper loop.
func (k *Keeper) Start(at time.Time) {
	k.Sim.Schedule(at, k.ServiceName+":keeper_start", func() {
		k.tick()
	})
}

func (k *Keeper) tick() {
	now := k.Sim.Now()

	// First, let the pool process MaxConnLifetime expiry
	k.Pool.Tick(now)

	stats := k.Pool.Stats()
	deficit := k.Target - stats.Open

	if deficit <= 0 {
		// Pool is at or above target, schedule next tick
		k.Sim.After(k.TickInterval, k.ServiceName+":keeper_tick", func() {
			k.tick()
		})
		return
	}

	// Ratio-based batching: create up to MaxConnsPerTick connections per tick.
	// This matches pool_keeper.go behavior where MaxConnsPerTick is calculated
	// as ceil(poolSize/120), capped at 10. The batch size is designed to handle
	// peak expiry rates during connection lifecycle rotation.
	toCreate := deficit
	if toCreate > k.MaxConnsPerTick {
		toCreate = k.MaxConnsPerTick
	}

	// Create connections one at a time within the batch, each respecting
	// the global rate limiter. This matches production behavior.
	//
	// If rate-limited mid-batch, we stop and retry after the next second.
	// This is correct - the global limit (100/sec) is the hard constraint,
	// while MaxConnsPerTick (up to 10) is just the local desire.
	created := 0
	for i := 0; i < toCreate; i++ {
		ok, retryAfter := k.Limiter.Acquire(now, k.Endpoint, 1)
		if !ok {
			k.Metrics.RecordLimiterDeny(now, k.ServiceName)
			// Rate limited - retry sooner than normal tick
			k.Sim.After(retryAfter, k.ServiceName+":keeper_retry", func() {
				k.tick()
			})
			k.retries++
			return
		}

		if err := k.Pool.OpenNewConn(now); err != nil {
			// Pool full - shouldn't happen if target <= maxOpen
			break
		}

		created++
		k.topups++
		k.Metrics.RecordKeeperOpen(now, k.ServiceName)
	}

	// Schedule next tick
	k.Sim.After(k.TickInterval, k.ServiceName+":keeper_tick", func() {
		k.tick()
	})
}

// Stats returns keeper statistics.
func (k *Keeper) Stats() (topups, retries int) {
	return k.topups, k.retries
}
