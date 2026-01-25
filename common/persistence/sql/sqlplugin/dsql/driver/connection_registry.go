package driver

import (
	"database/sql/driver"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionRegistry tracks all connections created by the token-refreshing driver.
// This enables the connection refresher to know when connections were created
// and trigger replacement of old connections.
type ConnectionRegistry struct {
	mu          sync.RWMutex
	connections map[uint64]*TrackedConnection
	nextID      atomic.Uint64
}

// TrackedConnection holds metadata about a connection.
type TrackedConnection struct {
	ID        uint64
	CreatedAt time.Time
}

// NewConnectionRegistry creates a new connection registry.
func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{
		connections: make(map[uint64]*TrackedConnection),
	}
}

// Register adds a connection to the registry and returns its tracking info.
func (r *ConnectionRegistry) Register(conn driver.Conn) *TrackedConnection {
	id := r.nextID.Add(1)
	tracked := &TrackedConnection{
		ID:        id,
		CreatedAt: time.Now(),
	}

	r.mu.Lock()
	r.connections[id] = tracked
	r.mu.Unlock()

	return tracked
}

// Unregister removes a connection from the registry.
func (r *ConnectionRegistry) Unregister(id uint64) {
	r.mu.Lock()
	delete(r.connections, id)
	r.mu.Unlock()
}

// ConnectionsOlderThan returns all connections older than the specified age.
func (r *ConnectionRegistry) ConnectionsOlderThan(age time.Duration) []*TrackedConnection {
	r.mu.RLock()
	defer r.mu.RUnlock()

	cutoff := time.Now().Add(-age)
	var old []*TrackedConnection
	for _, tc := range r.connections {
		if tc.CreatedAt.Before(cutoff) {
			old = append(old, tc)
		}
	}
	return old
}

// Count returns the number of tracked connections.
func (r *ConnectionRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.connections)
}

// OldestConnection returns the oldest tracked connection, or nil if none.
func (r *ConnectionRegistry) OldestConnection() *TrackedConnection {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var oldest *TrackedConnection
	for _, tc := range r.connections {
		if oldest == nil || tc.CreatedAt.Before(oldest.CreatedAt) {
			oldest = tc
		}
	}
	return oldest
}

// Stats returns registry statistics.
func (r *ConnectionRegistry) Stats() (count int, oldestAge time.Duration) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count = len(r.connections)
	if count == 0 {
		return count, 0
	}

	var oldest time.Time
	for _, tc := range r.connections {
		if oldest.IsZero() || tc.CreatedAt.Before(oldest) {
			oldest = tc.CreatedAt
		}
	}
	return count, time.Since(oldest)
}
