package sqlplugin

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/server/common/clock"
)

// IDGenerator provides distributed ID generation for databases that don't support sequences
type IDGenerator interface {
	NextID(ctx context.Context, table string) (int64, error)
}

// SnowflakeIDGenerator generates distributed IDs using a Snowflake-like algorithm
// Format: [timestamp(41 bits)][node_id(10 bits)][sequence(12 bits)]
type SnowflakeIDGenerator struct {
	nodeID     int64
	sequence   int64
	lastTime   int64
	timeSource clock.TimeSource
	mu         sync.Mutex // Add mutex for thread safety
}

const (
	// Epoch timestamp (2020-01-01 00:00:00 UTC) to maximize ID space
	temporalEpoch = 1577836800000 // milliseconds

	// Bit allocation
	timestampBits = 41
	nodeIDBits    = 10
	sequenceBits  = 12

	// Max values
	maxNodeID   = (1 << nodeIDBits) - 1   // 1023
	maxSequence = (1 << sequenceBits) - 1 // 4095

	// Bit shifts
	nodeIDShift    = sequenceBits
	timestampShift = sequenceBits + nodeIDBits
)

// NewSnowflakeIDGenerator creates a new Snowflake ID generator
func NewSnowflakeIDGenerator(nodeID int64, timeSource clock.TimeSource) (*SnowflakeIDGenerator, error) {
	if nodeID < 0 || nodeID > maxNodeID {
		return nil, fmt.Errorf("nodeID must be between 0 and %d, got %d", maxNodeID, nodeID)
	}

	return &SnowflakeIDGenerator{
		nodeID:     nodeID,
		timeSource: timeSource,
	}, nil
}

// NextID generates the next unique ID
func (g *SnowflakeIDGenerator) NextID(ctx context.Context, table string) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	timestamp := g.timeSource.Now().UnixMilli()

	// Handle clock going backwards
	if timestamp < g.lastTime {
		return 0, fmt.Errorf("clock moved backwards: last=%d, current=%d", g.lastTime, timestamp)
	}

	// Same millisecond - increment sequence
	if timestamp == g.lastTime {
		g.sequence++
		if g.sequence > maxSequence {
			// Wait for next millisecond
			for timestamp <= g.lastTime {
				time.Sleep(time.Millisecond)
				timestamp = g.timeSource.Now().UnixMilli()
			}
			g.sequence = 0
		}
		g.lastTime = timestamp
		return g.generateID(timestamp, g.sequence), nil
	}

	// New millisecond - reset sequence
	g.sequence = 0
	g.lastTime = timestamp
	return g.generateID(timestamp, 0), nil
}

func (g *SnowflakeIDGenerator) generateID(timestamp, sequence int64) int64 {
	// Adjust timestamp to our epoch
	timestamp -= temporalEpoch

	// Combine components: [timestamp][nodeID][sequence]
	return (timestamp << timestampShift) | (g.nodeID << nodeIDShift) | sequence
}

// RandomIDGenerator generates IDs using cryptographically secure random numbers
// Simpler than Snowflake but may have collision risk under extreme load
type RandomIDGenerator struct{}

// NewRandomIDGenerator creates a new random ID generator
func NewRandomIDGenerator() *RandomIDGenerator {
	return &RandomIDGenerator{}
}

// NextID generates a random 63-bit positive integer
func (g *RandomIDGenerator) NextID(ctx context.Context, table string) (int64, error) {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return 0, fmt.Errorf("failed to generate random ID: %w", err)
	}

	// Convert to int64 and ensure positive
	id := int64(binary.BigEndian.Uint64(bytes[:]))
	if id < 0 {
		id = -id
	}

	return id, nil
}

// GetNodeIDFromHostname generates a node ID from hostname for Snowflake generator
func GetNodeIDFromHostname(hostname string) int64 {
	if hostname == "" {
		// Fallback to random node ID
		var bytes [2]byte
		if _, err := rand.Read(bytes[:]); err != nil {
			// If random generation fails, use a deterministic fallback
			return 1
		}
		return int64(binary.BigEndian.Uint16(bytes[:])) % (maxNodeID + 1)
	}

	// Simple hash of hostname to node ID
	hash := int64(0)
	for _, b := range []byte(hostname) {
		hash = hash*31 + int64(b)
	}

	// Ensure positive result by taking absolute value, then modulo
	if hash < 0 {
		hash = -hash
	}

	return hash % (maxNodeID + 1)
}
