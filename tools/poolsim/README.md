# DSQL Pool Simulator

A discrete event simulator for validating DSQL connection pool behavior under various conditions. This simulator models the actual pool management implementation in `temporal-dsql`:

- **Pool Warmup**: Staggered connection creation at startup
- **Pool Keeper**: Edge-triggered maintenance that tops up the pool
- **MaxConnLifetime**: Go's `database/sql` closes connections after this duration
- **Rate Limiting**: DSQL's cluster-wide 100 connections/second limit
- **Workload Simulation**: Models database queries consuming connections

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

The simulator uses discrete event simulation - simulated time advances instantly between events, so a 30-minute simulation completes in seconds.

### Components Modeled

1. **Pool Warmup** (`pool_warmup.go` behavior)
   - Creates connections with staggered delays
   - Respects rate limiter during warmup
   - Connections get individual creation times (implicit jitter)

2. **Pool Keeper** (`pool_keeper.go` behavior)
   - Ticks every 1 second
   - Creates up to `MaxConnsPerTick` connections when `open < target`
   - Edge-triggered: only acts when there's a deficit

3. **Connection Lifecycle** (`session.go` defaults)
   - `MaxConnLifetime = 10 minutes`: Go closes connections after this
   - `MaxConnIdleTime = 0`: Disabled to prevent pool decay
   - Connections expire individually based on creation time

4. **Rate Limiter**
   - Models DSQL's 100 connections/second cluster-wide limit
   - All services share the same rate limit budget

5. **Workload Simulation** (`workload.go`)
   - Models database queries consuming connections
   - Configurable concurrency target (`maxInUse`)
   - Connection hold time with jitter
   - Tracks pool waits when connections unavailable

## Configuration

```yaml
seed: 42                          # Random seed for reproducibility
endpoint: cluster.dsql.on.aws     # DSQL endpoint (for rate limiter grouping)

duration: 20m                     # Simulation duration
sampleEvery: 1s                   # Metrics sampling interval

limiterPerSec: 100                # DSQL cluster rate limit

# Assertions (checked at end)
assertMaxConnectsPerSec: 100      # Must not exceed rate limit
assertConvergeWithin: 60s         # All pools reach target within this
assertStableFor: 5s               # Pools must stay at target for this duration
assertMaxClosurePerSec: 100       # Detect thundering herd

scenario:
  kind: cold_start                # cold_start | rolling_restart | mass_drop

services:
  - name: history
    targetOpen: 50                # Pool target size
    maxOpen: 50                   # MaxOpenConnections
    
    # Pool Warmup settings
    warmupStagger: 2m             # Spread warmup over this duration
    
    # Pool Keeper settings  
    keeperTick: 1s                # How often keeper checks pool
    maxConnsPerTick: 5            # Max connections per keeper tick
    
    # Connection lifecycle
    maxConnLifetime: 10m          # Go closes connections after this
    
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
- Warmup convergence time
- Rate limit compliance during startup
- Connection age distribution

### rolling_restart
Services restart one-by-one. Tests:
- Recovery time per service
- Rate limit contention during recovery
- Impact on other services

### mass_drop
Sudden connection loss (network event). Tests:
- Recovery from partial pool loss
- Rate limit contention during mass recovery

## Output

CSV output includes per-second samples:

| Column | Description |
|--------|-------------|
| `t_rfc3339` | Timestamp |
| `service` | Service name |
| `open` | Current open connections |
| `target` | Target pool size |
| `in_use` | Connections currently borrowed by workload |
| `closed_lifetime` | Connections closed by MaxConnLifetime |
| `closed_server` | Connections closed by server/network |
| `warmup_created` | Connections created during warmup |
| `keeper_created` | Connections created by Pool Keeper |
| `limiter_denies` | Rate limit denials |
| `pool_waits` | Times workload had to wait for a connection |
| `work_started` | Work items started |
| `work_completed` | Work items completed |

## Assertions

The simulator validates behavior with configurable assertions:

| Assertion | Description |
|-----------|-------------|
| `assertMaxConnectsPerSec` | Global connection rate must not exceed this (default: 100) |
| `assertConvergeWithin` | All pools must reach target within this duration |
| `assertStableFor` | After reaching target, pools must stay stable for this duration |
| `assertMaxClosurePerSec` | Detects thundering herd by limiting closure rate |

The `assertStableFor` assertion is particularly useful for detecting pool decay - if connections drop below target after convergence, the assertion fails.

## Comparison with Production

| Aspect | Simulator | Production |
|--------|-----------|------------|
| Connection lifecycle | Explicit tracking | Go's `database/sql` |
| Rate limiting | Simulated per-second | Driver-level blocking |
| Warmup | Modeled stagger | `pool_warmup.go` |
| Keeper | Modeled ticks | `pool_keeper.go` |
| Time | Discrete events | Real time |

The simulator validates the algorithm, not the exact implementation. Use it to:
- Validate configuration before deployment
- Test edge cases (mass drops, rolling restarts)
- Prove convergence within time bounds
- Detect thundering herd scenarios
- Verify pool stability under workload
