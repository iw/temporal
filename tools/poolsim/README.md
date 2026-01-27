# DSQL Reservoir Simulator

A discrete event simulator for validating DSQL connection reservoir behavior under various conditions. This simulator models the actual reservoir implementation in `temporal-dsql`:

- **Reservoir**: Channel-based connection buffer with fast checkout
- **Continuous Refiller**: Back-to-back connection creation with rate limiter as ONLY throttle
- **Proactive Expiry Scanner**: Background goroutine evicting connections before they expire
- **Guard Window**: Connections discarded if remaining lifetime < guard window
- **Rate Limiting**: DSQL's cluster-wide 100 connections/second limit
- **Workload Simulation**: Models database queries consuming connections

## Core Design Principles

| Principle | Description |
|-----------|-------------|
| **Fast Checkout** | Sub-millisecond checkout from channel buffer (hot path) |
| **Continuous Refill** | Rate limiter is the ONLY throttle - no artificial delays |
| **Proactive Expiry** | Background scanner evicts connections before they expire |
| **Eviction Callback** | Connections release lease on discard (not modeled in sim) |

## Quick Start

```bash
# Run with default config
go run ./tools/poolsim

# Run with custom config
go run ./tools/poolsim -config tools/poolsim/scenarios/400wps.yaml

# Output to CSV for analysis
go run ./tools/poolsim -config tools/poolsim/scenarios/local.yaml -out results.csv
```

## How It Works

The simulator uses discrete event simulation - simulated time advances instantly between events, so a 20-minute simulation completes in seconds.

### Components Modeled

1. **Reservoir** (`reservoir.go` behavior)
   - Channel-based buffer holding ready connections
   - FIFO checkout with guard window validation
   - Connections discarded if expired or within guard window
   - Tracks checkout latency, empty events, discard reasons

2. **Continuous Refiller** (`reservoir_refiller.go` behavior)
   - Runs back-to-back `openOne()` calls
   - Rate limiter is the ONLY throttle - no artificial delays
   - Brief idle check (100ms) when at target capacity
   - Backoff (250ms) only on connection creation failure

3. **Expiry Scanner** (second background goroutine)
   - Scans reservoir every 1 second
   - Evicts connections within guard window
   - Eager eviction for clustered expiry times
   - Creates refill demand for continuous refiller

4. **Connection Lifecycle**
   - `BaseLifetime = 11 minutes`: Base connection lifetime
   - `Jitter = 2 minutes`: Random jitter added to each connection
   - `GuardWindow = 45 seconds`: Discard if remaining < this
   - Connections expire individually based on creation time + jitter

5. **Rate Limiter**
   - Models DSQL's 100 connections/second cluster-wide limit
   - All services share the same rate limit budget

6. **Workload Simulation**
   - Models database queries consuming connections
   - Configurable concurrency target (`maxInUse`)
   - Connection hold time with jitter
   - Tracks empty events when reservoir has no connections

## Configuration

```yaml
seed: 42                          # Random seed for reproducibility
endpoint: cluster.dsql.on.aws     # DSQL endpoint (for rate limiter grouping)

duration: 20m                     # Simulation duration
sampleEvery: 1s                   # Metrics sampling interval

limiterPerSec: 100                # DSQL cluster rate limit

# Assertions (checked at end)
assertMaxConnectsPerSec: 100      # Must not exceed rate limit
assertConvergeWithin: 60s         # All reservoirs reach target within this
assertStableFor: 5s               # Reservoirs must stay at target for this duration
assertZeroEmptyEvents: true       # No checkout attempts should find empty reservoir

scenario:
  kind: cold_start                # cold_start | rolling_restart | mass_drop

services:
  - name: history
    targetReady: 50               # Target reservoir size
    guardWindow: 45s              # Discard if remaining lifetime < this
    baseLifetime: 11m             # Base connection lifetime
    jitter: 2m                    # Lifetime jitter (0 to jitter added)
    
    startDelay: 0s                # Delay before service starts
    
    # Workload simulation (optional)
    workload:
      enabled: true               # Enable workload simulation
      maxInUse: 20                # Target concurrent connections in use
      holdTime: 10ms              # How long each query holds a connection
      holdJitter: 5ms             # Random jitter added to hold time
      spawnEvery: 200ms           # How often to spawn new work
      arrivalBurst: 5             # Max work items per spawn tick
      maxWait: 10s                # Max wait for connection before giving up
```

## Scenarios

### cold_start
All services start from zero connections. Tests:
- Reservoir fill time
- Rate limit compliance during startup
- Connection age distribution with jitter

### rolling_restart
Services restart one-by-one. Tests:
- Recovery time per service
- Rate limit contention during recovery
- Impact on other services

### mass_drop
Sudden connection loss (network event). Tests:
- Recovery from partial reservoir loss
- Rate limit contention during mass recovery

## Output

CSV output includes per-second samples:

| Column | Description |
|--------|-------------|
| `t_rfc3339` | Timestamp |
| `service` | Service name |
| `size` | Current connections in reservoir |
| `target` | Target reservoir size |
| `in_use` | Connections currently checked out |
| `checkouts` | Total successful checkouts |
| `empty_events` | Checkout attempts when reservoir empty |
| `discards` | Total connections discarded |
| `discard_expired` | Discards due to expiry |
| `discard_guard` | Discards due to guard window |
| `discard_full` | Discards due to reservoir full |
| `refills` | Connections created by refiller |
| `refill_failures` | Failed refill attempts |
| `scan_evictions` | Connections evicted by expiry scanner |
| `limiter_denies` | Rate limit denials |
| `work_started` | Work items started |
| `work_completed` | Work items completed |

## Assertions

The simulator validates behavior with configurable assertions:

| Assertion | Description |
|-----------|-------------|
| `assertMaxConnectsPerSec` | Global connection rate must not exceed this (default: 100) |
| `assertConvergeWithin` | All reservoirs must reach target within this duration |
| `assertStableFor` | After reaching target, reservoirs must stay stable for this duration |
| `assertZeroEmptyEvents` | No checkout attempts should find an empty reservoir |

The `assertZeroEmptyEvents` assertion is critical - if the reservoir ever runs empty during checkout, it means the refiller isn't keeping up with demand.

## Comparison with Production

| Aspect | Simulator | Production |
|--------|-----------|------------|
| Reservoir | Slice-based buffer | Channel-based buffer |
| Refiller | Simulated continuous loop | Goroutine with rate limiter |
| Expiry Scanner | Simulated periodic scan | Goroutine every 1 second |
| Rate limiting | Simulated per-second | Driver-level blocking |
| Time | Discrete events | Real time |

The simulator validates the algorithm, not the exact implementation. Use it to:
- Validate configuration before deployment
- Test edge cases (mass drops, rolling restarts)
- Prove convergence within time bounds
- Verify zero empty events under workload
- Analyze discard patterns (expired vs guard vs full)

## Example Output

```
Running reservoir simulation for 20m0s...

=== Reservoir Simulation Summary ===
Duration: 20m0s
Services: 4
Rate limit: 100/sec

Service: history
  Final size: 50 / 50
  Checkouts: 0
  Empty events: 0
  Refills: 100
  Discards: 50 (expired=0, guard=50, full=0)

Max connects/sec: 100 (limit: 100)
Total empty events: 0
OK
```

The output shows:
- All services reach their target of 50 connections
- Zero empty events (reservoir always has connections available)
- ~100 refills per service over 20 minutes (initial 50 + ~50 replacements)
- 50 discards per service due to guard window eviction (proactive expiry working)
