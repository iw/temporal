package main

import (
	"time"
)

// Conn represents a simulated database connection in the reservoir.
type Conn struct {
	ID        int64
	CreatedAt time.Time
	Lifetime  time.Duration // Base + jitter
}

// ExpiresAt returns when this connection expires.
func (c *Conn) ExpiresAt() time.Time {
	return c.CreatedAt.Add(c.Lifetime)
}

// RemainingLifetime returns how much time is left before expiry.
func (c *Conn) RemainingLifetime(now time.Time) time.Duration {
	remaining := c.ExpiresAt().Sub(now)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// ReservoirStats contains reservoir statistics at a point in time.
type ReservoirStats struct {
	Size           int   // Current connections in reservoir
	Target         int   // Target size
	InUse          int   // Connections currently checked out
	Checkouts      int64 // Total successful checkouts
	EmptyEvents    int64 // Checkout attempts when empty
	Discards       int64 // Total discards
	DiscardExpired int64 // Discards due to expiry
	DiscardGuard   int64 // Discards due to guard window
	DiscardFull    int64 // Discards due to reservoir full
	Refills        int64 // Connections created by refiller
	RefillFailures int64 // Failed refill attempts
}

// Reservoir simulates the connection reservoir.
// This models the channel-based reservoir from reservoir.go.
type Reservoir struct {
	Name string

	Target       int           // Target reservoir size
	GuardWindow  time.Duration // Discard if remaining lifetime < this
	BaseLifetime time.Duration
	Jitter       time.Duration

	nextID int64
	conns  []*Conn         // Simulates the channel buffer
	inUse  map[int64]*Conn // Connections currently checked out

	// Stats
	checkouts      int64
	emptyEvents    int64
	discards       int64
	discardExpired int64
	discardGuard   int64
	discardFull    int64
	refills        int64
	refillFailures int64
}

// NewReservoir creates a new simulated reservoir.
func NewReservoir(name string, target int, guardWindow, baseLifetime, jitter time.Duration) *Reservoir {
	return &Reservoir{
		Name:         name,
		Target:       target,
		GuardWindow:  guardWindow,
		BaseLifetime: baseLifetime,
		Jitter:       jitter,
		conns:        make([]*Conn, 0, target),
		inUse:        make(map[int64]*Conn),
	}
}

// Stats returns current reservoir statistics.
func (r *Reservoir) Stats() ReservoirStats {
	return ReservoirStats{
		Size:           len(r.conns),
		Target:         r.Target,
		InUse:          len(r.inUse),
		Checkouts:      r.checkouts,
		EmptyEvents:    r.emptyEvents,
		Discards:       r.discards,
		DiscardExpired: r.discardExpired,
		DiscardGuard:   r.discardGuard,
		DiscardFull:    r.discardFull,
		Refills:        r.refills,
		RefillFailures: r.refillFailures,
	}
}

// Size returns current number of connections in reservoir.
func (r *Reservoir) Size() int {
	return len(r.conns)
}

// TryCheckout attempts to get a connection from the reservoir.
// Returns (conn, true) if successful, (nil, false) if empty or all expired.
func (r *Reservoir) TryCheckout(now time.Time) (*Conn, bool) {
	for len(r.conns) > 0 {
		// Pop from front (FIFO)
		conn := r.conns[0]
		r.conns = r.conns[1:]

		remaining := conn.RemainingLifetime(now)

		// Discard if expired
		if remaining == 0 {
			r.discards++
			r.discardExpired++
			continue
		}

		// Discard if within guard window
		if remaining < r.GuardWindow {
			r.discards++
			r.discardGuard++
			continue
		}

		// Valid connection - checkout
		r.checkouts++
		r.inUse[conn.ID] = conn
		return conn, true
	}

	// Reservoir empty
	r.emptyEvents++
	return nil, false
}

// Return returns a connection to the reservoir.
func (r *Reservoir) Return(conn *Conn, now time.Time) {
	if conn == nil {
		return
	}

	// Remove from in-use
	delete(r.inUse, conn.ID)

	remaining := conn.RemainingLifetime(now)

	// Discard if expired
	if remaining == 0 {
		r.discards++
		r.discardExpired++
		return
	}

	// Discard if within guard window
	if remaining < r.GuardWindow {
		r.discards++
		r.discardGuard++
		return
	}

	// Discard if reservoir full
	if len(r.conns) >= r.Target {
		r.discards++
		r.discardFull++
		return
	}

	// Return to reservoir
	r.conns = append(r.conns, conn)
}

// AddNew adds a newly created connection to the reservoir.
// Called by the refiller after successfully creating a connection.
func (r *Reservoir) AddNew(conn *Conn) bool {
	if len(r.conns) >= r.Target {
		r.discards++
		r.discardFull++
		return false
	}
	r.conns = append(r.conns, conn)
	r.refills++
	return true
}

// RecordRefillFailure records a failed refill attempt.
func (r *Reservoir) RecordRefillFailure() {
	r.refillFailures++
}

// ScanAndEvict scans the reservoir and evicts expired/expiring connections.
// Returns the number of connections evicted.
func (r *Reservoir) ScanAndEvict(now time.Time) int {
	evicted := 0
	valid := make([]*Conn, 0, len(r.conns))

	for _, conn := range r.conns {
		remaining := conn.RemainingLifetime(now)

		// Evict if expired
		if remaining == 0 {
			r.discards++
			r.discardExpired++
			evicted++
			continue
		}

		// Evict if within guard window
		if remaining < r.GuardWindow {
			r.discards++
			r.discardGuard++
			evicted++
			continue
		}

		// Keep valid connections
		valid = append(valid, conn)
	}

	r.conns = valid
	return evicted
}

// DropAll removes all connections (simulates service restart).
func (r *Reservoir) DropAll() int {
	count := len(r.conns) + len(r.inUse)
	r.conns = r.conns[:0]
	r.inUse = make(map[int64]*Conn)
	return count
}

// DropFraction removes a fraction of connections.
func (r *Reservoir) DropFraction(frac float64) int {
	if frac <= 0 {
		return 0
	}

	toDrop := int(float64(len(r.conns)) * frac)
	if toDrop <= 0 {
		toDrop = 1
	}
	if toDrop > len(r.conns) {
		toDrop = len(r.conns)
	}

	// Drop from front
	r.conns = r.conns[toDrop:]
	return toDrop
}

// CreateConn creates a new connection with jittered lifetime.
func (r *Reservoir) CreateConn(now time.Time, jitterRand func() time.Duration) *Conn {
	r.nextID++
	lifetime := r.BaseLifetime
	if r.Jitter > 0 {
		lifetime += jitterRand()
	}
	return &Conn{
		ID:        r.nextID,
		CreatedAt: now,
		Lifetime:  lifetime,
	}
}
