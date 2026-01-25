package main

import (
	"fmt"
	"time"
)

// CloseReason indicates why a connection was closed.
type CloseReason string

const (
	CloseLifetime CloseReason = "lifetime" // MaxConnLifetime expired
	CloseServer   CloseReason = "server"   // Server/network closed connection
)

// Conn represents a simulated database connection.
type Conn struct {
	ID        int64
	OpenedAt  time.Time
	ExpiresAt time.Time // When MaxConnLifetime will close this connection
	InUse     bool      // Whether connection is currently borrowed
}

// PoolStats contains pool statistics at a point in time.
type PoolStats struct {
	MaxOpen        int
	Open           int
	Idle           int
	InUse          int
	ClosedLifetime int64
	ClosedServer   int64
}

// BorrowResult is returned when attempting to borrow a connection.
type BorrowResult struct {
	ConnID          int64
	WaitFor         time.Duration // if >0, caller should retry later
	Opened          bool          // true if we opened a new connection
	DeniedByLimiter bool          // true if wait is due to rate limit
}

// Pool simulates a database connection pool with MaxConnLifetime behavior.
// This models Go's database/sql pool with our DSQL configuration:
// - MaxConnLifetime = 10 minutes (connections closed after this)
// - MaxConnIdleTime = 0 (disabled, connections never closed due to idle)
// - MaxIdleConns = MaxConns (pool stays at max size)
type Pool struct {
	Name string

	MaxOpen         int
	MaxConnLifetime time.Duration

	nextID int64
	conns  map[int64]*Conn

	closedLifetime int64
	closedServer   int64

	// Wait statistics
	waitCount   int64
	waitSeconds float64
}

// NewPool creates a new simulated pool.
func NewPool(name string, maxOpen int, maxConnLifetime time.Duration) *Pool {
	return &Pool{
		Name:            name,
		MaxOpen:         maxOpen,
		MaxConnLifetime: maxConnLifetime,
		conns:           make(map[int64]*Conn),
	}
}

// Stats returns current pool statistics.
func (p *Pool) Stats() PoolStats {
	idle := 0
	inUse := 0
	for _, c := range p.conns {
		if c.InUse {
			inUse++
		} else {
			idle++
		}
	}
	return PoolStats{
		MaxOpen:        p.MaxOpen,
		Open:           len(p.conns),
		Idle:           idle,
		InUse:          inUse,
		ClosedLifetime: p.closedLifetime,
		ClosedServer:   p.closedServer,
	}
}

// OpenNewConn creates a new connection in the pool.
// Returns error if pool is at max capacity.
func (p *Pool) OpenNewConn(now time.Time) error {
	if len(p.conns) >= p.MaxOpen {
		return fmt.Errorf("pool %s: max open reached (%d)", p.Name, p.MaxOpen)
	}

	p.nextID++
	id := p.nextID

	// Connection expires after MaxConnLifetime
	// No jitter here - the staggered warmup provides implicit jitter
	// by creating connections at different times
	expiresAt := now.Add(p.MaxConnLifetime)

	p.conns[id] = &Conn{
		ID:        id,
		OpenedAt:  now,
		ExpiresAt: expiresAt,
	}
	return nil
}

// CloseConn closes a specific connection.
func (p *Pool) CloseConn(id int64, reason CloseReason) {
	if _, ok := p.conns[id]; !ok {
		return
	}
	delete(p.conns, id)
	switch reason {
	case CloseLifetime:
		p.closedLifetime++
	case CloseServer:
		p.closedServer++
	}
}

// Tick processes time-based events (MaxConnLifetime expiry).
// This models Go's database/sql behavior of closing connections
// that have exceeded MaxConnLifetime.
func (p *Pool) Tick(now time.Time) {
	for id, c := range p.conns {
		if !now.Before(c.ExpiresAt) {
			p.CloseConn(id, CloseLifetime)
		}
	}
}

// DropFraction forcibly closes a fraction of connections.
// Used to simulate network events or server-side disconnects.
func (p *Pool) DropFraction(frac float64) int {
	if frac <= 0 {
		return 0
	}
	open := len(p.conns)
	if open == 0 {
		return 0
	}

	toDrop := int(float64(open) * frac)
	if toDrop <= 0 {
		toDrop = 1
	}
	if toDrop > open {
		toDrop = open
	}

	// Drop first N connections (deterministic for reproducibility)
	dropped := 0
	for id := range p.conns {
		if dropped >= toDrop {
			break
		}
		p.CloseConn(id, CloseServer)
		dropped++
	}
	return dropped
}

// DropAll closes all connections (simulates service restart).
func (p *Pool) DropAll() int {
	count := len(p.conns)
	for id := range p.conns {
		p.CloseConn(id, CloseServer)
	}
	return count
}

// findIdle returns an idle connection ID if available.
func (p *Pool) findIdle() (int64, bool) {
	for id, c := range p.conns {
		if !c.InUse {
			return id, true
		}
	}
	return 0, false
}

// Borrow attempts to get a connection from the pool.
// This models database/sql's connection acquisition behavior:
// 1. Try to reuse an idle connection
// 2. If pool not full, open a new connection (rate-limited)
// 3. If pool exhausted, wait
func (p *Pool) Borrow(now time.Time, limiter RateLimiter, endpoint string) BorrowResult {
	// 1. Try to reuse an idle connection
	if id, ok := p.findIdle(); ok {
		p.conns[id].InUse = true
		return BorrowResult{ConnID: id}
	}

	// 2. If pool can grow, try to open (rate-limited)
	if len(p.conns) < p.MaxOpen {
		ok, retryAfter := limiter.Acquire(now, endpoint, 1)
		if !ok {
			p.waitCount++
			p.waitSeconds += retryAfter.Seconds()
			return BorrowResult{WaitFor: retryAfter, DeniedByLimiter: true}
		}
		_ = p.OpenNewConn(now)
		if id, ok := p.findIdle(); ok {
			p.conns[id].InUse = true
			return BorrowResult{ConnID: id, Opened: true}
		}
	}

	// 3. Pool exhausted - wait for a connection to be released
	waitFor := 50 * time.Millisecond
	p.waitCount++
	p.waitSeconds += waitFor.Seconds()
	return BorrowResult{WaitFor: waitFor}
}

// Release returns a connection to the pool.
func (p *Pool) Release(id int64) {
	if c, ok := p.conns[id]; ok {
		c.InUse = false
	}
}

// WaitCount returns the number of times callers had to wait.
func (p *Pool) WaitCount() int64 {
	return p.waitCount
}

// WaitSeconds returns total wait time in seconds.
func (p *Pool) WaitSeconds() float64 {
	return p.waitSeconds
}
