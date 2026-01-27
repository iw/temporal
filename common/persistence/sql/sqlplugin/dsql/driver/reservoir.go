package driver

import (
	"context"
	"database/sql/driver"
	"sync"
	"time"
)

// LeaseReleaser releases a previously acquired global connection lease.
// Implemented in the dsql package and injected into the driver.
type LeaseReleaser interface {
	Release(ctx context.Context, leaseID string) error
}

// ReservoirMetrics defines the interface for reservoir-specific metrics.
// This interface is used by the reservoir components to record metrics.
type ReservoirMetrics interface {
	// RecordReservoirSize records the current reservoir size
	RecordReservoirSize(size int)

	// RecordReservoirTarget records the target reservoir size
	RecordReservoirTarget(target int)

	// IncReservoirCheckouts increments the counter for successful checkouts
	IncReservoirCheckouts()

	// IncReservoirEmpty increments the counter for checkout attempts when reservoir is empty
	IncReservoirEmpty()

	// IncReservoirDiscards increments the counter for discarded connections with reason
	IncReservoirDiscards(reason string)

	// IncReservoirRefills increments the counter for connections created by refiller
	IncReservoirRefills()

	// IncReservoirRefillFailures increments the counter for failed connection creates with reason
	IncReservoirRefillFailures(reason string)

	// RecordCheckoutLatency records the time taken to checkout a connection from reservoir
	// This should be near-zero in steady state (just a channel receive)
	RecordCheckoutLatency(d time.Duration)
}

// noOpReservoirMetrics is a no-op implementation of ReservoirMetrics for when metrics are disabled
type noOpReservoirMetrics struct{}

func (n *noOpReservoirMetrics) RecordReservoirSize(size int)             {}
func (n *noOpReservoirMetrics) RecordReservoirTarget(target int)         {}
func (n *noOpReservoirMetrics) IncReservoirCheckouts()                   {}
func (n *noOpReservoirMetrics) IncReservoirEmpty()                       {}
func (n *noOpReservoirMetrics) IncReservoirDiscards(reason string)       {}
func (n *noOpReservoirMetrics) IncReservoirRefills()                     {}
func (n *noOpReservoirMetrics) IncReservoirRefillFailures(reason string) {}
func (n *noOpReservoirMetrics) RecordCheckoutLatency(d time.Duration)    {}

// PhysicalConn is a physical database connection held in the reservoir.
// It wraps an underlying driver.Conn plus metadata used for expiration and global lease release.
//
// Connection age is tracked from creation time rather than using a fixed expiry time.
// This allows computing remaining lifetime at checkout, which is more accurate when
// connections sit in the reservoir for varying amounts of time.
type PhysicalConn struct {
	Conn      driver.Conn
	CreatedAt time.Time     // When the connection was established
	Lifetime  time.Duration // Total lifetime (base + jitter)
	LeaseID   string
}

// ExpiresAt returns the absolute expiration time for this connection.
// This is computed from CreatedAt + Lifetime.
func (pc *PhysicalConn) ExpiresAt() time.Time {
	if pc.CreatedAt.IsZero() {
		return time.Time{}
	}
	return pc.CreatedAt.Add(pc.Lifetime)
}

// RemainingLifetime returns how much time is left before this connection expires.
// Returns 0 if the connection is already expired.
func (pc *PhysicalConn) RemainingLifetime(now time.Time) time.Duration {
	if pc.CreatedAt.IsZero() || pc.Lifetime <= 0 {
		return 0
	}
	age := now.Sub(pc.CreatedAt)
	remaining := pc.Lifetime - age
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Age returns how long this connection has been alive.
func (pc *PhysicalConn) Age(now time.Time) time.Duration {
	if pc.CreatedAt.IsZero() {
		return 0
	}
	return now.Sub(pc.CreatedAt)
}

// Reservoir holds ready-to-use physical connections.
//
// IMPORTANT: All operations required by driver.Open() are non-blocking.
// Reservoir refilling (which may block on global limiters) happens in the background.
type Reservoir struct {
	ready       chan *PhysicalConn
	guardWindow time.Duration
	leaseRel    LeaseReleaser
	logFunc     LogFunc
	metrics     ReservoirMetrics

	// stats
	mu        sync.Mutex
	discards  int64
	emptyHits int64
}

func NewReservoir(capacity int, guardWindow time.Duration, leaseRel LeaseReleaser, logFunc LogFunc, metrics ReservoirMetrics) *Reservoir {
	if capacity <= 0 {
		capacity = 1
	}
	if metrics == nil {
		metrics = &noOpReservoirMetrics{}
	}
	return &Reservoir{
		ready:       make(chan *PhysicalConn, capacity),
		guardWindow: guardWindow,
		leaseRel:    leaseRel,
		logFunc:     logFunc,
		metrics:     metrics,
	}
}

// TryCheckout attempts to retrieve a ready connection without blocking.
// It computes remaining lifetime at checkout time and discards connections
// that don't have sufficient remaining lifetime (within guard window).
func (r *Reservoir) TryCheckout(now time.Time) (*PhysicalConn, bool) {
	select {
	case pc := <-r.ready:
		if pc == nil {
			r.noteEmpty()
			return nil, false
		}
		// Compute remaining lifetime and discard if insufficient
		remaining := pc.RemainingLifetime(now)
		if remaining > 0 && remaining < r.guardWindow {
			r.discard(pc, "insufficient_remaining_lifetime")
			r.noteEmpty()
			return nil, false
		}
		// Discard if remaining lifetime is 0 (expired or marked bad with Lifetime=0)
		if remaining == 0 {
			r.discard(pc, "expired_on_checkout")
			r.noteEmpty()
			return nil, false
		}
		// Successful checkout - emit metrics
		r.metrics.IncReservoirCheckouts()
		r.metrics.RecordReservoirSize(len(r.ready))
		return pc, true
	default:
		r.noteEmpty()
		return nil, false
	}
}

// WaitCheckout waits up to timeout for a ready connection.
// This provides a brief blocking wait to smooth out transient empty reservoir conditions.
// Returns (nil, false) if timeout is reached or only expired connections are available.
func (r *Reservoir) WaitCheckout(timeout time.Duration) (*PhysicalConn, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case pc := <-r.ready:
		if pc == nil {
			return nil, false
		}
		now := time.Now().UTC()
		// Compute remaining lifetime and discard if insufficient
		remaining := pc.RemainingLifetime(now)
		if remaining > 0 && remaining < r.guardWindow {
			r.discard(pc, "insufficient_remaining_lifetime_on_wait")
			return nil, false
		}
		// Discard if remaining lifetime is 0 (expired or marked bad with Lifetime=0)
		if remaining == 0 {
			r.discard(pc, "expired_on_wait")
			return nil, false
		}
		// Successful checkout after wait - emit metrics
		r.metrics.IncReservoirCheckouts()
		r.metrics.RecordReservoirSize(len(r.ready))
		return pc, true
	case <-timer.C:
		return nil, false
	}
}

// Return attempts to return a connection to the reservoir without blocking.
// If the reservoir is full or the connection has insufficient remaining lifetime, the connection is discarded.
func (r *Reservoir) Return(pc *PhysicalConn, now time.Time) {
	if pc == nil {
		return
	}
	// Compute remaining lifetime and discard if insufficient
	remaining := pc.RemainingLifetime(now)
	if remaining > 0 && remaining < r.guardWindow {
		r.discard(pc, "insufficient_remaining_lifetime_on_return")
		return
	}
	// Discard if remaining lifetime is 0 (expired or marked bad with Lifetime=0)
	if remaining == 0 {
		r.discard(pc, "expired_on_return")
		return
	}

	select {
	case r.ready <- pc:
		// Successfully returned - update size metric
		r.metrics.RecordReservoirSize(len(r.ready))
		return
	default:
		// Reservoir full; discard to avoid blocking.
		r.discard(pc, "reservoir_full")
	}
}

func (r *Reservoir) Len() int {
	return len(r.ready)
}

// ScanAndEvict drains the reservoir, discards expired/expiring connections,
// and returns valid connections back to the reservoir.
// Returns the number of connections evicted.
func (r *Reservoir) ScanAndEvict(now time.Time) int {
	evicted := 0
	currentLen := len(r.ready)

	// Drain and re-add, discarding expired connections
	for i := 0; i < currentLen; i++ {
		select {
		case pc := <-r.ready:
			if pc == nil {
				continue
			}
			remaining := pc.RemainingLifetime(now)
			// Discard if expired or within guard window
			if remaining == 0 {
				r.discard(pc, "expired_on_scan")
				evicted++
				continue
			}
			if remaining < r.guardWindow {
				r.discard(pc, "expiring_soon_on_scan")
				evicted++
				continue
			}
			// Still valid - put back
			select {
			case r.ready <- pc:
				// OK
			default:
				// Shouldn't happen, but discard if full
				r.discard(pc, "reservoir_full_on_scan")
				evicted++
			}
		default:
			// Channel empty, done
			break
		}
	}

	if evicted > 0 {
		r.metrics.RecordReservoirSize(len(r.ready))
		if r.logFunc != nil {
			r.logFunc("DSQL reservoir scan complete", "evicted", evicted, "remaining", len(r.ready))
		}
	}

	return evicted
}

func (r *Reservoir) noteEmpty() {
	r.mu.Lock()
	r.emptyHits++
	r.mu.Unlock()
	// Emit empty reservoir metric
	r.metrics.IncReservoirEmpty()
	r.metrics.RecordReservoirSize(0)
	if r.logFunc != nil {
		r.logFunc("DSQL reservoir empty", "empty_hits_total", r.emptyHits)
	}
}

func (r *Reservoir) discard(pc *PhysicalConn, reason string) {
	r.mu.Lock()
	r.discards++
	r.mu.Unlock()

	// Emit discard metric with reason
	r.metrics.IncReservoirDiscards(reason)

	if r.logFunc != nil {
		r.logFunc("DSQL reservoir discard", "reason", reason, "discards_total", r.discards)
	}

	if pc.Conn != nil {
		_ = pc.Conn.Close()
	}
	if r.leaseRel != nil && pc.LeaseID != "" {
		_ = r.leaseRel.Release(context.Background(), pc.LeaseID)
	}
}
