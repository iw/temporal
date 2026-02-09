# Connection Reservoir Design

## Overview

The Connection Reservoir is an advanced connection management mode for the DSQL plugin that addresses the fundamental mismatch between DSQL's cluster-wide connection rate limit (100 connections/second) and the bursty nature of connection pool refill when connections expire due to `MaxConnLifetime`.

## Core Design Principles

The reservoir is built around four fundamental requirements:

| Principle | Description | Why It Matters |
|-----------|-------------|----------------|
| **Fast Checkout** | Sub-millisecond checkout from reservoir | The hot path - request latency depends on this |
| **Proactive Expiry** | Don't let stale connections sit in reservoir | Prevents handing out connections that will expire mid-transaction |
| **Continuous Refill** | Always keep reservoir full | Connection availability is paramount |
| **Eviction Callback** | Release lease on discard | Global connection count must stay accurate |

These principles drive all design decisions. The reservoir exists to ensure connections are **always available** without blocking on rate limiters.

## Problem Statement

DSQL has a **cluster-wide connection rate limit of 100 connections/second**. When connections expire due to `MaxConnLifetime`, the pool needs to replace them. If many connections expire at once (burst expiry), the pool shrinks because the refill rate can't keep up with the rate limit.

**Current behavior without reservoir:**
- Pool warmup creates N connections
- Without sufficient stagger, connections have similar ages
- After `MaxConnLifetime`, many expire within a short window
- Pool Keeper tries to refill at 1-10 connections/tick
- Global rate limit (100/sec) constrains all services
- Pool shrinks during burst expiry, causing latency spikes

## Pool Lifecycle Ownership

In reservoir mode, the reservoir owns connection lifecycle — not `database/sql`. The pool is a pass-through:

- **The reservoir owns all connections.** It creates them, tracks their age, and discards them when they approach expiry.
- **`database/sql` is a pass-through.** When the pool calls `driver.Open()`, it receives a pre-created connection from the reservoir channel. When the pool closes a connection, the reservoir reclaims the lease.
- **`MaxConnLifetime` is disabled** (set to 0) because the reservoir manages lifetime via `BASE_LIFETIME` + jitter. Letting `database/sql` also expire connections would cause double-eviction and unnecessary churn.
- **`MaxConnIdleTime` is disabled** (set to 0) to prevent the pool from shrinking during low-traffic periods. The reservoir keeps connections warm regardless of query activity.

This means the pool size stays constant at `maxOpen` and never decays. Connection replacement happens one-at-a-time in the background refiller, paced by the rate limiter — never in the request path.

## Ephemeral Pools

Temporal creates short-lived database connections during startup for operations like schema version checks (`VerifyCompatibleVersion`) and metadata initialization. These are not service pools — they open, run a few queries, and close immediately.

The DSQL plugin recognises these via `config.SQL.PoolSizeHint = "ephemeral"` (set in `version_checker.go` and `fx.go`). When the reservoir sees this hint, it uses a minimal configuration:

| Setting | Ephemeral | Service (default) |
|---------|-----------|-------------------|
| Target ready | 5 | `maxOpen` (typically 50) |
| Low watermark | 5 | `maxOpen` |
| Initial fill timeout | 10s | 30s |
| Inflight limit | 2 | 8 |
| Distributed conn leasing | Skipped | Enabled (if configured) |

This avoids wasting rate limit budget and DynamoDB calls on pools that exist for seconds. The ephemeral pool still benefits from the reservoir's IAM token caching and rate limiting — it just uses far fewer resources.

## Solution: Connection Reservoir

A **reservoir** is a buffer of pre-created connections that sits between the rate-limited connection creation and the pool's bursty demand.

```
                    Global Rate Limit (100/sec)
                           │
                           ▼
┌──────────────────────────────────────────────────┐
│              RESERVOIR (per service)             │
│                                                  │
│  Continuously filled by background refiller      │
│  Maintains buffer of "ready" connections         │
│                                                  │
│  ┌─────────────────────────────────────────┐    │
│  │  Channel buffer (capacity = targetReady) │    │
│  │  [conn1] [conn2] [conn3] ... [connN]     │    │
│  └─────────────────────────────────────────┘    │
│                                                  │
└──────────────────────────────────────────────────┘
                           │
                           ▼ (instant - no rate limit)
┌──────────────────────────────────────────────────┐
│              POOL (Go's database/sql)            │
│                                                  │
│  Calls driver.Open() when it needs a connection  │
│  Driver returns connection from reservoir        │
│  No waiting for rate limit                       │
└──────────────────────────────────────────────────┘
```

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Reservoir Driver                          │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
│  │  Reservoir  │◄───│   Driver    │◄───│  database/sql   │ │
│  │  (channel)  │    │   Open()    │    │     Pool        │ │
│  └─────────────┘    └─────────────┘    └─────────────────┘ │
│         ▲                                                   │
│         │                                                   │
│  ┌──────┴──────┐    ┌─────────────┐                        │
│  │  Refiller   │    │   Expiry    │  (background goroutines)│
│  │  Loop       │    │  Scanner    │                        │
│  └─────────────┘    └─────────────┘                        │
│         │                                                   │
│         ▼                                                   │
│  ┌─────────────┐    ┌─────────────┐                        │
│  │ Rate Limiter│    │   Lease     │  (optional)            │
│  │  (local or  │    │  Manager    │                        │
│  │ distributed)│    │ (DynamoDB)  │                        │
│  └─────────────┘    └─────────────┘                        │
└─────────────────────────────────────────────────────────────┘
```

### Data Structures

```go
// Reservoir holds ready-to-use physical connections
type Reservoir struct {
    ready       chan *PhysicalConn  // Buffered channel
    guardWindow time.Duration       // Discard if remaining lifetime within this
    leaseRel    LeaseReleaser       // For releasing global leases (eviction callback)
    metrics     ReservoirMetrics
}

// PhysicalConn is a physical database connection held in the reservoir
type PhysicalConn struct {
    Conn      driver.Conn
    CreatedAt time.Time     // When connection was established
    Lifetime  time.Duration // Total lifetime (base + jitter)
    LeaseID   string        // For global connection count tracking
}

// Key methods:
// - TryCheckout(now) - Non-blocking checkout (sub-ms)
// - Return(pc, now) - Non-blocking return to reservoir
// - ScanAndEvict(now) - Proactive expiry scanning
```

### Key Design Decisions

1. **Channel-based Buffer**: Using a buffered channel provides natural FIFO ordering and thread-safe access without explicit locking for the hot path.

2. **Age Tracking**: Connection age is tracked from creation time rather than using a fixed expiry time. This allows computing remaining lifetime at checkout, which is more accurate when connections sit in the reservoir for varying amounts of time.

3. **Guard Window**: Connections within `guardWindow` of expiry are discarded on checkout/return. This prevents handing out connections that will expire mid-transaction.

4. **Non-blocking Operations**: All operations required by `driver.Open()` are non-blocking. Reservoir refilling (which may block on global limiters) happens in the background.

## Refiller and Expiry Scanner

The reservoir uses two background goroutines to maintain connection availability:

### Continuous Refiller

The refiller runs back-to-back `openOne()` calls whenever the reservoir is below target. **The rate limiter is the ONLY throttle** - no artificial delays, no warmup/steady-state modes.

```go
// Refiller pacing constants
const (
    IdleCheckInterval = 100 * time.Millisecond  // Check interval when at target
    FailureBackoff    = 250 * time.Millisecond  // Backoff after failure
)

func (r *reservoirRefiller) loop() {
    for {
        ready := r.res.Len()
        need := r.cfg.TargetReady - ready

        if need <= 0 {
            // At target capacity - brief check interval
            sleep(IdleCheckInterval)
            continue
        }

        // Create one connection - rate limiter controls pacing
        err := r.openOne(ctx)
        if err != nil {
            sleep(FailureBackoff)
            continue
        }
        // No delay - immediately try to create next connection
    }
}
```

**Key design decisions:**
- **No artificial delays**: Rate limiter is the only throttle
- **Sequential creation**: One connection at a time for reliability
- **Immediate retry**: After successful creation, immediately check if more needed
- **Backoff on failure**: Brief pause after errors to avoid hammering failing resources

### Proactive Expiry Scanner

The expiry scanner runs every second and proactively evicts connections that are expired or approaching expiry. This ensures stale connections don't sit in the reservoir waiting for checkout.

```go
const ExpiryScanInterval = 1 * time.Second

func (r *reservoirRefiller) expiryScanner() {
    ticker := time.NewTicker(ExpiryScanInterval)
    for {
        select {
        case <-r.stopC:
            return
        case <-ticker.C:
            evicted := r.res.ScanAndEvict(time.Now().UTC())
            // Evicted connections trigger refiller to replace them
        }
    }
}
```

**ScanAndEvict algorithm:**
1. Drain the channel into a temporary slice
2. For each connection, check remaining lifetime
3. If expired or within guard window → discard (releases lease)
4. If still valid → put back in channel
5. Return count of evicted connections

```go
func (r *Reservoir) ScanAndEvict(now time.Time) int {
    evicted := 0
    currentLen := len(r.ready)

    for i := 0; i < currentLen; i++ {
        select {
        case pc := <-r.ready:
            remaining := pc.RemainingLifetime(now)
            if remaining == 0 || remaining < r.guardWindow {
                r.discard(pc, "expiring_soon_on_scan")
                evicted++
                continue
            }
            // Still valid - put back
            r.ready <- pc
        default:
            break
        }
    }
    return evicted
}
```

**Why proactive scanning matters:**
- Without scanning, expired connections only discovered at checkout time
- Checkout failures waste time and trigger retries
- Proactive eviction keeps the reservoir "fresh"
- Eager eviction handles clustered expiry times (connections created together expire together)

## Global Connection Count Limiting

DSQL has a default limit of **10,000 concurrent connections per cluster** (can be raised via AWS support request). When running multiple Temporal services (Frontend, History, Matching, Worker) across multiple instances, it's easy to exceed this limit without coordination.

### Why Global Limiting is Needed

Consider a typical production deployment:
- 4 Temporal services × 10 instances each = 40 service instances
- Each instance has 2 pools (default + visibility) × 50 connections = 100 connections per instance
- Total: 40 × 100 = 4,000 connections

Without coordination, scaling up or a burst of connection creation could easily exceed the default 10,000 connection limit.

### Slot Block Manager (Recommended)

The **Slot Block Manager** provides distributed connection limiting using a block-based allocation strategy that avoids hot partition issues in DynamoDB.

Instead of incrementing a single counter per connection (which creates a hot partition), the Slot Block Manager:
1. Pre-allocates **blocks** of connection slots (default: 100 slots per block)
2. Each service acquires one or more blocks at startup
3. Once a block is owned, connections can be created without DynamoDB calls
4. TTL-based crash recovery ensures blocks are released if a service crashes

```
┌─────────────────────────────────────────────────────────────────┐
│                     DynamoDB Table                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Block Items (100 blocks × 100 slots = 10,000 total slots):    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ pk: "connslots#cluster.dsql.us-east-1.on.aws#block-0"   │   │
│  │ owner_id: "a1b2c3d4e5f6..."                             │   │
│  │ ttl_epoch: 1706284980                                    │   │
│  │ slots: 100                                               │   │
│  │ service_name: "history"                                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ pk: "connslots#cluster.dsql.us-east-1.on.aws#block-1"   │   │
│  │ owner_id: "f6e5d4c3b2a1..."                             │   │
│  │ ttl_epoch: 1706284990                                    │   │
│  │ slots: 100                                               │   │
│  │ service_name: "matching"                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ pk: "connslots#cluster.dsql.us-east-1.on.aws#block-2"   │   │
│  │ owner_id: ""  (unowned - available)                     │   │
│  │ ttl_epoch: 0                                             │   │
│  │ slots: 100                                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ... (100 blocks total)                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Why Block-Based Allocation?

| Approach | DynamoDB Calls | Hot Partition | Crash Recovery |
|----------|----------------|---------------|----------------|
| Per-connection counter | 2 per connection | Yes (single item) | TTL on lease items |
| **Slot blocks** | 1 per block at startup | No (100 partition keys) | TTL on blocks |

With slot blocks:
- **No hot partition**: Each block has its own partition key
- **Minimal DynamoDB calls**: Only at startup and for TTL renewal
- **Fast local tracking**: Once blocks are owned, slot allocation is in-memory

#### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Enable slot block manager |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | DynamoDB table name (required if enabled) |
| `DSQL_SLOT_BLOCK_SIZE` | `100` | Slots per block |
| `DSQL_SLOT_BLOCK_COUNT` | `100` | Total number of blocks (100 × 100 = 10k slots) |
| `DSQL_SLOT_BLOCK_TTL` | `3m` | TTL for crash recovery |
| `DSQL_SLOT_BLOCK_RENEW_INTERVAL` | `1m` | How often to renew TTL |

#### How It Works

1. **Startup**: Service calculates how many blocks it needs based on `DSQL_RESERVOIR_TARGET_READY`
2. **Block Acquisition**: Tries to acquire blocks using conditional PutItem (only if unowned or TTL expired)
3. **Slot Tracking**: Tracks used slots in-memory with atomic counter
4. **TTL Renewal**: Background goroutine renews TTL on owned blocks every minute
5. **Shutdown**: Releases blocks by clearing `owner_id`

```go
// Block acquisition condition
ConditionExpression: "attribute_not_exists(pk) OR owner_id = :empty OR ttl_epoch < :now"
```

#### Failure Modes

| Scenario | Behavior |
|----------|----------|
| DynamoDB unavailable at startup | Block acquisition fails, service starts without global limiting |
| Service crash | Blocks become available after TTL expires (3 min) |
| All blocks owned | New services cannot acquire slots, log warning |
| TTL renewal fails | Block may be taken by another service after TTL expires |

#### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dsql_slot_blocks_owned` | Gauge | Number of blocks owned by this service |
| `dsql_slot_blocks_slots_used` | Gauge | Number of slots currently in use |

## Distributed Rate Limiting

DSQL has a **cluster-wide connection rate limit of 100 connections/second** with a **burst capacity of 1,000 connections**. The plugin provides two rate limiting modes to coordinate across service instances.

### Token Bucket Rate Limiter (Recommended)

The **Token Bucket Rate Limiter** uses DynamoDB to coordinate rate limiting across all service instances, taking advantage of DSQL's burst capacity.

```
┌─────────────────────────────────────────────────────────────────┐
│                     Token Bucket Model                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Bucket (single DynamoDB item per endpoint):                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ pk: "dsql_connect_bucket#cluster.dsql.us-east-1.on.aws" │   │
│  │ tokens_milli: 850000  (850 tokens × 1000)               │   │
│  │ last_refill_ms: 1706284800000                           │   │
│  │ rate_milli: 100000    (100 tokens/sec × 1000)           │   │
│  │ capacity_milli: 1000000 (1000 tokens × 1000)            │   │
│  │ ttl_epoch: 1706288400                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  • Refills at 100 tokens/second (DSQL sustained rate)          │
│  • Capacity of 1000 tokens (DSQL burst capacity)               │
│  • Uses milli-tokens (×1000) to avoid floating point           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

#### Why Token Bucket?

| Approach | Burst Support | Coordination | Complexity |
|----------|---------------|--------------|------------|
| Per-second counter | No | Yes | Simple |
| **Token bucket** | Yes (1000) | Yes | Moderate |
| Local rate limiter | No | No | Simple |

The token bucket allows:
- **Fast initial fill**: Use burst capacity (1000) for rapid startup
- **Sustained rate**: Settle to 100/sec after burst exhausted
- **Fair sharing**: All services draw from the same bucket

#### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED` | `false` | Enable distributed rate limiting |
| `DSQL_DISTRIBUTED_RATE_LIMITER_TABLE` | - | DynamoDB table name |
| `DSQL_TOKEN_BUCKET_ENABLED` | `false` | Use token bucket (vs per-second counter) |
| `DSQL_TOKEN_BUCKET_RATE` | `100` | Tokens per second (DSQL sustained rate) |
| `DSQL_TOKEN_BUCKET_CAPACITY` | `1000` | Bucket capacity (DSQL burst capacity) |
| `DSQL_TOKEN_BUCKET_MAX_WAIT` | `30s` | Maximum wait time for a token |

#### How It Works

1. **Token Acquisition**: Atomic DynamoDB UpdateItem with condition
2. **Refill Calculation**: `new_tokens = min(capacity, current + elapsed_ms × rate / 1000)`
3. **Decrement**: Subtract 1 token on successful acquisition
4. **Retry**: If bucket empty, wait and retry with backoff

```go
// Condition: after refill, tokens >= 1
ConditionExpression: `
    attribute_not_exists(pk) OR
    (tokens_milli + elapsed_ms × rate_milli / 1000) >= 1000
`
```

#### Logging

The token bucket limiter logs:
- **Debug**: Token acquired (only if wait > 10ms or retries > 1)
- **Warn**: Token acquire failure or timeout

### Per-Second Counter (Legacy)

The legacy distributed rate limiter uses a simple per-second counter. It does not support burst capacity.

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED` | `false` | Enable distributed rate limiting |
| `DSQL_DISTRIBUTED_RATE_LIMITER_TABLE` | - | DynamoDB table name |
| `DSQL_DISTRIBUTED_RATE_LIMITER_LIMIT` | `100` | Connections per second |

### Local Rate Limiter

When distributed rate limiting is disabled, each service instance uses a local token bucket rate limiter. This requires manual partitioning of the 100/sec budget across instances.

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_CONNECTION_RATE_LIMIT` | `10` | Connections per second per instance |
| `DSQL_CONNECTION_BURST_LIMIT` | `100` | Burst capacity per instance |

## In-Flight Semaphore

The refiller uses an **in-flight semaphore** to limit concurrent `Open()` calls. This prevents handshake pile-ups even when the rate limiter allows burst.

### Why Limit Concurrency?

TCP/TLS handshakes take time (~50-200ms). If the rate limiter allows 1000 connections in burst, launching 1000 concurrent handshakes would:
- Overwhelm the network stack
- Create connection timeouts
- Waste rate limit budget on failed connections

The in-flight semaphore limits concurrent handshakes to a reasonable number (default: 8).

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_RESERVOIR_INFLIGHT_LIMIT` | `8` | Max concurrent Open() calls |

### Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dsql_refiller_inflight` | Gauge | Current number of in-flight Open() calls |

## Configuration

### Environment Variables

The reservoir is configured entirely through environment variables, allowing operators to tune behavior without code changes.

#### Reservoir Core Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_RESERVOIR_ENABLED` | `false` | Boolean | Enable reservoir mode. When `false`, the plugin uses the standard token-refreshing driver with pool warmup. |
| `DSQL_RESERVOIR_TARGET_READY` | `maxOpen` | Integer | Target number of connections to maintain in the reservoir. Defaults to the pool's `maxOpen` setting. |
| `DSQL_RESERVOIR_LOW_WATERMARK` | `maxOpen` | Integer | Threshold below which the refiller uses aggressive (warmup) pacing. Defaults to `maxOpen`. |
| `DSQL_RESERVOIR_BASE_LIFETIME` | `11m` | Duration | Base lifetime for connections before they are discarded. Should be less than DSQL's 60-minute connection limit. |
| `DSQL_RESERVOIR_LIFETIME_JITTER` | `2m` | Duration | Random jitter added to each connection's lifetime to prevent synchronized expiry. Actual lifetime = base ± jitter/2. |
| `DSQL_RESERVOIR_GUARD_WINDOW` | `45s` | Duration | Time before expiry when connections are considered too old to hand out. Prevents mid-transaction expiry. |
| `DSQL_RESERVOIR_INFLIGHT_LIMIT` | `8` | Integer | Maximum concurrent Open() calls in the refiller. Prevents handshake pile-ups. |

#### Distributed Rate Limiting Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED` | `false` | Boolean | Enable DynamoDB-backed distributed rate limiting. |
| `DSQL_DISTRIBUTED_RATE_LIMITER_TABLE` | - | String | DynamoDB table name for rate limiting. Required if enabled. |
| `DSQL_TOKEN_BUCKET_ENABLED` | `false` | Boolean | Use token bucket algorithm (recommended). If false, uses per-second counter. |
| `DSQL_TOKEN_BUCKET_RATE` | `100` | Integer | Token refill rate (tokens/second). Should match DSQL's sustained rate. |
| `DSQL_TOKEN_BUCKET_CAPACITY` | `1000` | Integer | Maximum tokens in bucket. Should match DSQL's burst capacity. |
| `DSQL_TOKEN_BUCKET_MAX_WAIT` | `30s` | Duration | Maximum time to wait for a token before failing. |

#### Distributed Connection Limiting Configuration (Slot Blocks)

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Boolean | Enable DynamoDB-backed global connection count limiting. |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | String | DynamoDB table name for slot blocks. Required if enabled. |
| `DSQL_SLOT_BLOCK_SIZE` | `100` | Integer | Number of connection slots per block. |
| `DSQL_SLOT_BLOCK_COUNT` | `100` | Integer | Total number of blocks (100 × 100 = 10k slots). |
| `DSQL_SLOT_BLOCK_TTL` | `3m` | Duration | TTL for crash recovery. Blocks become available after TTL expires. |
| `DSQL_SLOT_BLOCK_RENEW_INTERVAL` | `1m` | Duration | How often to renew TTL on owned blocks. |

### Configuration Details

#### `DSQL_RESERVOIR_ENABLED`

Controls whether the reservoir mode is active. When enabled:
- Connections are pre-created by a background refiller
- `driver.Open()` returns instantly from the reservoir buffer
- Connection lifetime is managed by the reservoir, not `database/sql`

When disabled (default):
- Standard token-refreshing driver is used
- Pool warmup creates connections at startup
- Pool Keeper maintains pool size

**Example:**
```bash
export DSQL_RESERVOIR_ENABLED=true
```

#### `DSQL_RESERVOIR_TARGET_READY`

The number of connections the refiller tries to maintain in the reservoir. This should typically match your pool's `maxOpen` setting to ensure connections are always available.

**Considerations:**
- Higher values provide more buffer against burst demand
- Lower values reduce idle connection overhead
- Must be ≥ `DSQL_RESERVOIR_LOW_WATERMARK`

**Example:**
```bash
export DSQL_RESERVOIR_TARGET_READY=50
```

#### `DSQL_RESERVOIR_LOW_WATERMARK`

When the reservoir size drops below this threshold, the refiller switches to aggressive (warmup) pacing, using the full rate limit budget to recover quickly.

**Considerations:**
- Set equal to `TARGET_READY` for maximum responsiveness
- Set lower (e.g., 50% of target) to reduce rate limit usage during normal operation
- The refiller smoothly transitions between warmup and steady-state pacing

**Example:**
```bash
export DSQL_RESERVOIR_LOW_WATERMARK=25  # Aggressive refill below 25 connections
```

#### `DSQL_RESERVOIR_BASE_LIFETIME`

The base lifetime for connections in the reservoir. After this duration (plus jitter), connections are discarded and replaced.

**Considerations:**
- Must be less than DSQL's 60-minute connection limit
- 11 minutes is recommended to allow for jitter and guard window
- Shorter lifetimes increase rate limit usage but ensure fresher IAM tokens

**Example:**
```bash
export DSQL_RESERVOIR_BASE_LIFETIME=11m
```

#### `DSQL_RESERVOIR_LIFETIME_JITTER`

Random jitter added to each connection's lifetime. This prevents all connections from expiring at the same time (thundering herd).

**How it works:**
- Each connection gets a random offset in the range `[-jitter/2, +jitter/2]`
- With base=11m and jitter=2m, actual lifetimes range from 10m to 12m
- Connections expire gradually over the jitter window

**Example:**
```bash
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
```

#### `DSQL_RESERVOIR_GUARD_WINDOW`

Connections with remaining lifetime less than this value are discarded on checkout or return. This prevents handing out connections that might expire during a transaction.

**Considerations:**
- Should be longer than your longest expected transaction
- 45 seconds is recommended for typical Temporal workloads
- Longer values waste more connection lifetime but are safer

**Example:**
```bash
export DSQL_RESERVOIR_GUARD_WINDOW=45s
```

#### `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED`

Enables DynamoDB-backed global connection count limiting. When enabled, the refiller acquires a lease before creating each connection, ensuring the cluster-wide limit is respected.

**When to enable:**
- Multiple Temporal services share a DSQL cluster
- Total connections across all services might exceed 10,000
- You need cluster-wide coordination

**When to disable:**
- Single service with dedicated DSQL cluster
- Connection count is well below the limit
- DynamoDB is not available

**Example:**
```bash
export DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
```

#### `DSQL_DISTRIBUTED_CONN_LEASE_TABLE`

The DynamoDB table name for lease tracking. Required if distributed leasing is enabled.

**Table requirements:**
- Partition key: `pk` (String)
- TTL attribute: `ttl_epoch` (Number)
- On-demand billing recommended

**Example:**
```bash
export DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
```

#### `DSQL_DISTRIBUTED_CONN_LIMIT`

The maximum number of connections allowed cluster-wide. This should match DSQL's connection limit (10,000 by default).

**Considerations:**
- Set to DSQL's actual limit (10,000)
- Consider leaving headroom for burst capacity
- Can be set lower to reserve connections for other applications

**Example:**
```bash
export DSQL_DISTRIBUTED_CONN_LIMIT=10000
```

### Sizing Guidelines

| Pool Size | Reservoir Target | Low Watermark | Rationale |
|-----------|------------------|---------------|-----------|
| 10 | 10 | 10 | Match pool size for small pools |
| 50 | 50 | 50 | Match pool size, aggressive refill always |
| 50 | 50 | 25 | Match pool size, aggressive refill below 50% |
| 100 | 100 | 100 | Match pool size |
| 500 | 500 | 250 | Large pools have natural distribution |

### Recommended Configurations

#### Development / Low Throughput

```bash
# Minimal configuration for development
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=10
export DSQL_RESERVOIR_LOW_WATERMARK=10
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s
```

#### Production / High Throughput (Single Cluster)

```bash
# Production configuration without distributed leasing
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=50
export DSQL_RESERVOIR_LOW_WATERMARK=50
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s
```

#### Production / Multi-Service (Shared Cluster)

```bash
# Production configuration with distributed rate limiting and slot blocks
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=50
export DSQL_RESERVOIR_LOW_WATERMARK=50
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s
export DSQL_RESERVOIR_INFLIGHT_LIMIT=8

# Enable token bucket rate limiting (recommended)
export DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED=true
export DSQL_DISTRIBUTED_RATE_LIMITER_TABLE=temporal-dsql-rate-limiter
export DSQL_TOKEN_BUCKET_ENABLED=true
export DSQL_TOKEN_BUCKET_RATE=100
export DSQL_TOKEN_BUCKET_CAPACITY=1000

# Enable slot block connection limiting
export DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
export DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
```

### ECS Task Definition Example

For ECS deployments, add these environment variables to your task definition:

```hcl
environment = [
  # Reservoir configuration
  { name = "DSQL_RESERVOIR_ENABLED", value = "true" },
  { name = "DSQL_RESERVOIR_TARGET_READY", value = "50" },
  { name = "DSQL_RESERVOIR_LOW_WATERMARK", value = "50" },
  { name = "DSQL_RESERVOIR_BASE_LIFETIME", value = "11m" },
  { name = "DSQL_RESERVOIR_LIFETIME_JITTER", value = "2m" },
  { name = "DSQL_RESERVOIR_GUARD_WINDOW", value = "45s" },
  
  # Distributed connection leasing (optional)
  { name = "DSQL_DISTRIBUTED_CONN_LEASE_ENABLED", value = "true" },
  { name = "DSQL_DISTRIBUTED_CONN_LEASE_TABLE", value = "temporal-dsql-conn-lease" },
  { name = "DSQL_DISTRIBUTED_CONN_LIMIT", value = "10000" },
]
```

### Docker Compose Example

For local Docker Compose deployments:

```yaml
services:
  temporal-history:
    environment:
      # Reservoir configuration
      DSQL_RESERVOIR_ENABLED: "true"
      DSQL_RESERVOIR_TARGET_READY: "50"
      DSQL_RESERVOIR_LOW_WATERMARK: "50"
      DSQL_RESERVOIR_BASE_LIFETIME: "11m"
      DSQL_RESERVOIR_LIFETIME_JITTER: "2m"
      DSQL_RESERVOIR_GUARD_WINDOW: "45s"
```

### Validation

The configuration is validated at startup:
- `TARGET_READY` is automatically adjusted to be ≥ `LOW_WATERMARK`
- `BASE_LIFETIME` defaults to 11m if set to 0 or negative
- `JITTER` defaults to 0 if set to negative
- `GUARD_WINDOW` defaults to 0 if set to negative

Invalid boolean values for `ENABLED` flags are treated as `false`.

## Sequence Diagrams

### Connection Checkout (Happy Path)

```
database/sql          reservoirDriver          Reservoir
     │                      │                      │
     │──Open(dsn)──────────>│                      │
     │                      │──TryCheckout(now)───>│
     │                      │<──(PhysicalConn)─────│
     │<──reservoirConn──────│                      │
     │                      │                      │
```

### Connection Checkout (Empty Reservoir)

```
database/sql          reservoirDriver          Reservoir
     │                      │                      │
     │──Open(dsn)──────────>│                      │
     │                      │──TryCheckout(now)───>│
     │                      │<──(nil, false)───────│
     │<──ErrReservoirEmpty──│                      │
     │                      │                      │
     │  (persistence layer retries)                │
```

**Note:** `Open()` is strictly non-blocking. When the reservoir is empty, it returns `ErrReservoirEmpty` — a custom sentinel error that signals transient backpressure. This is intentionally *not* `driver.ErrBadConn`, which would tell `database/sql` the connection is corrupted and trigger pool recreation. See [Why ErrReservoirEmpty](#why-errreservoirempty-not-drivererrbadconn) for the full rationale.

### Refiller Loop

```
Refiller              RateLimiter         LeaseManager         Reservoir
   │                      │                    │                   │
   │──Wait(ctx)──────────>│                    │                   │
   │<─────────────────────│                    │                   │
   │──Acquire(ctx)────────────────────────────>│                   │
   │<──(leaseID)───────────────────────────────│                   │
   │                      │                    │                   │
   │  (create connection with IAM token)       │                   │
   │                      │                    │                   │
   │──Return(PhysicalConn)─────────────────────────────────────────>│
   │                      │                    │                   │
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Reservoir empty | Return `ErrReservoirEmpty` immediately (non-blocking), persistence layer retries |
| Connection expired on checkout | Discard, return `ErrReservoirEmpty` |
| Connection error during use | Mark bad, discard on close |
| Rate limiter timeout | Refiller backs off, retries |
| Lease acquire fails (limit reached) | Refiller backs off, retries |
| DynamoDB unavailable | Fall back to local-only (no global limiting) |

### Why ErrReservoirEmpty (not driver.ErrBadConn)?

When the reservoir is empty, we return `ErrReservoirEmpty` instead of `driver.ErrBadConn`. This is a critical design decision that prevents cascading pool recreation:

**What `driver.ErrBadConn` does:**
1. `database/sql` interprets it as "this connection is corrupted"
2. It calls `DatabaseHandle.ConvertError`, which can trigger pool recreation
3. Pool recreation creates a *new* `database/sql` pool and a *new* reservoir
4. The old refiller goroutines are leaked — they keep running, consuming rate limit budget
5. The new reservoir starts filling from scratch, competing with the leaked refiller
6. Under sustained load, this cascades: each empty event creates another pool, each pool leaks another refiller

**What `ErrReservoirEmpty` does:**
1. The persistence layer sees a retryable error (not a corrupted connection)
2. It backs off and retries, giving the refiller time to replenish
3. No pool recreation, no goroutine leaks, no cascade
4. The reservoir self-heals as the refiller catches up

```go
// ErrReservoirEmpty is returned when the reservoir has no connections available.
// This is a transient backpressure signal, NOT a corrupted connection error.
// Returning driver.ErrBadConn here would trigger pool recreation cascades.
var ErrReservoirEmpty = errors.New("dsql reservoir empty: no connections available")
```

The persistence layer's retry logic handles `ErrReservoirEmpty` by backing off and retrying, giving the refiller time to replenish the reservoir. Under normal operation, empty events should be rare — the refiller keeps the reservoir full, and the brief 100ms blocking wait in `Open()` smooths out transient gaps.

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dsql_reservoir_size` | Gauge | service | Current reservoir size |
| `dsql_reservoir_target` | Gauge | service | Target reservoir size |
| `dsql_reservoir_checkouts_total` | Counter | service | Successful checkouts |
| `dsql_reservoir_empty_total` | Counter | service | Checkout when empty |
| `dsql_reservoir_discards_total` | Counter | service, reason | Discarded connections |
| `dsql_reservoir_refills_total` | Counter | service | Connections created |
| `dsql_reservoir_refill_failures_total` | Counter | service, reason | Failed creates |
| `dsql_reservoir_checkout_latency_milliseconds` | Histogram | service | Checkout latency (should be <1ms) |

### Discard Reasons

| Reason | Description |
|--------|-------------|
| `insufficient_remaining_lifetime` | Connection within guard window on checkout/return |
| `expired_on_checkout` | Connection already expired when checked out |
| `expired_on_return` | Connection expired while in use |
| `expired_on_scan` | Connection expired, found by expiry scanner |
| `expiring_soon_on_scan` | Connection within guard window, found by expiry scanner |
| `reservoir_full` | Connection returned but reservoir at capacity |

### Key Metrics to Watch

| Metric | Healthy Value | Alert Threshold |
|--------|---------------|-----------------|
| `dsql_reservoir_checkout_latency_milliseconds` p99 | < 1ms | > 10ms |
| `dsql_reservoir_size / dsql_reservoir_target` | > 0.9 | < 0.5 for 5 min |
| `rate(dsql_reservoir_empty_total[5m])` | 0 | > 0 for 2 min |
| `rate(dsql_reservoir_refill_failures_total[5m])` | 0 | > 0 for 5 min |

## When to Use Reservoir Mode

**Use reservoir mode when:**
- Running high-throughput workloads (>100 WPS)
- Multiple services share a DSQL cluster
- Connection expiry causes latency spikes
- Pool shrinkage is observed during burst expiry

**Stick with standard mode when:**
- Running low-throughput workloads
- Single service with dedicated DSQL cluster
- Connection pool is stable without intervention

## Troubleshooting

This section provides comprehensive guidance for diagnosing and resolving issues with the Connection Reservoir.

### Quick Diagnostic Checklist

Before diving into specific issues, run through this checklist:

1. **Is reservoir mode enabled?** Check `DSQL_RESERVOIR_ENABLED=true`
2. **Are services starting?** Look for "Reservoir refiller started" in logs
3. **Is the reservoir filling?** Check `dsql_reservoir_size` metric
4. **Are there errors?** Check `dsql_reservoir_refill_failures_total` metric
5. **Is DynamoDB accessible?** (if using distributed leasing)

### Common Issues and Solutions

#### Issue: Reservoir Not Filling at Startup

**Symptoms:**
- Services start but `dsql_reservoir_size` stays at 0 or very low
- High `dsql_reservoir_empty_total` counter
- Requests failing with connection errors

**Diagnostic Steps:**

1. **Check refiller logs:**
   ```
   grep "Reservoir refiller" /var/log/temporal/*.log
   ```
   Look for:
   - "Reservoir refiller started" - confirms refiller is running
   - "Refill failed" - indicates connection creation issues

2. **Check rate limiter:**
   ```promql
   # If using distributed rate limiter, check DynamoDB
   rate(dsql_reservoir_refill_failures_total{reason="rate_limit"}[5m])
   ```

3. **Check IAM token generation:**
   ```promql
   rate(dsql_reservoir_refill_failures_total{reason="token_provider"}[5m])
   ```

4. **Check DSQL connectivity:**
   ```bash
   # From the container/host
   nc -zv <cluster-endpoint> 5432
   ```

**Solutions:**

| Cause | Solution |
|-------|----------|
| Rate limiter too restrictive | Increase `DSQL_CONNECTION_RATE_LIMIT` or check distributed rate limiter table |
| IAM credentials expired | Refresh AWS credentials, check IAM role permissions |
| DSQL cluster unavailable | Check AWS console, verify endpoint and region |
| Network connectivity | Check security groups, VPC endpoints, NAT gateway |
| Global connection limit reached | Check DynamoDB counter, scale down other services |

#### Issue: Reservoir Draining Under Load

**Symptoms:**
- `dsql_reservoir_size` drops during high traffic
- Increasing `dsql_reservoir_empty_total`
- Latency spikes in application

**Diagnostic Steps:**

1. **Check checkout vs refill rate:**
   ```promql
   # Checkout rate
   rate(dsql_reservoir_checkouts_total[1m])
   
   # Refill rate
   rate(dsql_reservoir_refills_total[1m])
   ```
   If checkout rate >> refill rate, the reservoir can't keep up.

2. **Check discard rate:**
   ```promql
   sum by (reason) (rate(dsql_reservoir_discards_total[1m]))
   ```

3. **Check if connections are being returned:**
   - High `reservoir_full` discards = connections returning but reservoir at capacity
   - High `insufficient_remaining_lifetime` = connections expiring too fast

**Solutions:**

| Cause | Solution |
|-------|----------|
| Checkout rate exceeds refill capacity | Increase `DSQL_RESERVOIR_TARGET_READY`, add more service instances |
| Connections expiring too fast | Increase `DSQL_RESERVOIR_BASE_LIFETIME` |
| Guard window too large | Reduce `DSQL_RESERVOIR_GUARD_WINDOW` (but keep > longest transaction) |
| Rate limit constraining refill | Check cluster-wide rate limit usage, stagger service restarts |

#### Issue: High Connection Discard Rate

**Symptoms:**
- `dsql_reservoir_discards_total` increasing rapidly
- Reservoir size fluctuating
- Wasted rate limit budget

**Diagnostic Steps:**

1. **Identify discard reason:**
   ```promql
   topk(5, sum by (reason) (rate(dsql_reservoir_discards_total[5m])))
   ```

2. **Check connection age distribution:**
   - If most discards are `insufficient_remaining_lifetime`, connections are sitting too long in reservoir
   - If most discards are `reservoir_full`, checkout rate is lower than expected

**Solutions by Discard Reason:**

| Reason | Cause | Solution |
|--------|-------|----------|
| `insufficient_remaining_lifetime` | Guard window too large relative to lifetime | Reduce guard window or increase base lifetime |
| `expired_on_checkout` | Connections sitting too long in reservoir | Reduce target size or increase checkout rate |
| `expired_on_return` | Long-running transactions | Increase base lifetime |
| `reservoir_full` | Low checkout rate | Reduce target size to match actual demand |

#### Issue: Empty Reservoir Events

**Symptoms:**
- `dsql_reservoir_empty_total` counter increasing
- Application logs showing `ErrReservoirEmpty` or connection retry messages
- Intermittent latency spikes

**Diagnostic Steps:**

1. **Check if this is transient or persistent:**
   ```promql
   # Transient: occasional spikes
   # Persistent: continuous increase
   rate(dsql_reservoir_empty_total[1m])
   ```

2. **Check refiller health:**
   ```promql
   # Should be > 0 if refiller is working
   rate(dsql_reservoir_refills_total[1m])
   ```

3. **Check for refill failures:**
   ```promql
   sum by (reason) (rate(dsql_reservoir_refill_failures_total[1m]))
   ```

**Solutions:**

| Scenario | Solution |
|----------|----------|
| Transient during startup | Normal - wait for initial fill to complete |
| Transient during traffic spikes | Increase target size, add more instances |
| Persistent with refill failures | Fix underlying refill issue (see "Reservoir Not Filling") |
| Persistent without refill failures | Increase target size, reduce guard window |

#### Issue: Global Connection Limit Reached

**Symptoms:**
- `dsql_reservoir_refill_failures_total{reason="lease_acquire"}` increasing
- Multiple services unable to create connections
- DynamoDB counter at or near limit

**Diagnostic Steps:**

1. **Check DynamoDB counter:**
   ```bash
   aws dynamodb get-item \
     --table-name temporal-dsql-conn-lease \
     --key '{"pk": {"S": "dsqllease_counter#<your-endpoint>"}}' \
     --region <region>
   ```

2. **Check connection distribution across services:**
   ```promql
   sum by (service) (dsql_reservoir_size)
   ```

3. **Check for leaked leases (counter drift):**
   - Compare DynamoDB counter with sum of all `dsql_reservoir_size` metrics
   - If counter >> sum, there may be leaked leases from crashed services

**Solutions:**

| Cause | Solution |
|-------|----------|
| Legitimate high usage | Scale down services, reduce per-service pool size |
| Counter drift from crashes | Wait for TTL cleanup (3 min), or manually reset counter |
| Uneven distribution | Adjust per-service `DSQL_RESERVOIR_TARGET_READY` |

**Manual Counter Reset (Emergency):**
```bash
# WARNING: Only use if you're certain the counter is wrong
aws dynamodb update-item \
  --table-name temporal-dsql-conn-lease \
  --key '{"pk": {"S": "dsqllease_counter#<your-endpoint>"}}' \
  --update-expression "SET active = :val" \
  --expression-attribute-values '{":val": {"N": "0"}}' \
  --region <region>
```

#### Issue: DynamoDB Errors (Distributed Leasing)

**Symptoms:**
- `dsql_reservoir_refill_failures_total{reason="lease_acquire"}` increasing
- Errors mentioning "TransactionCanceledException" or "ProvisionedThroughputExceededException"

**Diagnostic Steps:**

1. **Check DynamoDB table health:**
   ```bash
   aws dynamodb describe-table --table-name temporal-dsql-conn-lease --region <region>
   ```

2. **Check CloudWatch metrics for the table:**
   - `ConsumedWriteCapacityUnits`
   - `ThrottledRequests`
   - `SystemErrors`

3. **Check TTL is enabled:**
   ```bash
   aws dynamodb describe-time-to-live --table-name temporal-dsql-conn-lease --region <region>
   ```

**Solutions:**

| Error | Solution |
|-------|----------|
| `ProvisionedThroughputExceededException` | Switch to on-demand billing or increase provisioned capacity |
| `ResourceNotFoundException` | Create the DynamoDB table (see setup script) |
| `AccessDeniedException` | Check IAM permissions for `dynamodb:TransactWriteItems` |
| TTL not enabled | Enable TTL on `ttl_epoch` attribute |

#### Issue: IAM Token Refresh Failures

**Symptoms:**
- `dsql_reservoir_refill_failures_total{reason="token_provider"}` increasing
- Errors mentioning "unable to generate auth token"

**Diagnostic Steps:**

1. **Check IAM role/credentials:**
   ```bash
   aws sts get-caller-identity
   ```

2. **Check DSQL permissions:**
   ```bash
   # Should have dsql:DbConnect and dsql:DbConnectAdmin
   aws iam get-role-policy --role-name <role-name> --policy-name <policy-name>
   ```

3. **Check token generation manually:**
   ```bash
   aws dsql generate-db-connect-admin-auth-token \
     --hostname <cluster-endpoint> \
     --region <region>
   ```

**Solutions:**

| Cause | Solution |
|-------|----------|
| Missing IAM permissions | Add `dsql:DbConnect` and `dsql:DbConnectAdmin` to role |
| Expired credentials | Refresh credentials, check IRSA/instance profile |
| Wrong region | Verify `AWS_REGION` environment variable |
| Clock skew | Sync system clock (IAM tokens are time-sensitive) |

### Log Messages Reference

#### Startup Logs (Normal)

```
# Reservoir mode enabled
Reservoir mode enabled, registering reservoir driver

# Refiller starting
Reservoir refiller started  target_ready=50 low_watermark=50 base_lifetime=11m0s

# Initial fill progress
Reservoir refiller: created connection  reservoir_size=1 target=50

# Initial fill complete
Reservoir initial fill complete  size=50 target=50 elapsed=12.3s
```

#### Warning Logs

```
# Initial fill timeout (service continues but may have issues)
Reservoir initial fill timeout  current=35 target=50

# Connection discarded
Reservoir: discarding connection  reason=insufficient_remaining_lifetime remaining=30s guard_window=45s

# Refill failure (will retry)
Reservoir refiller: failed to create connection  error="rate limit exceeded" attempt=3
```

#### Error Logs

```
# Lease acquire failure
Reservoir refiller: lease acquire failed  error="TransactionCanceledException: limit reached"

# Token provider failure
Reservoir refiller: token provider failed  error="unable to generate auth token"

# DSQL connection failure
Reservoir refiller: connection open failed  error="connection refused"
```

### Metrics-Based Monitoring

#### Key Metrics to Watch

| Metric | Healthy Value | Alert Threshold |
|--------|---------------|-----------------|
| `dsql_reservoir_size / dsql_reservoir_target` | > 0.9 | < 0.5 for 5 min |
| `rate(dsql_reservoir_empty_total[5m])` | 0 | > 0 for 2 min |
| `rate(dsql_reservoir_refill_failures_total[5m])` | 0 | > 0 for 5 min |
| `rate(dsql_reservoir_discards_total[5m])` | < 1/sec | > 5/sec for 5 min |

#### Grafana Dashboard Queries

```promql
# Reservoir health overview
dsql_reservoir_size
dsql_reservoir_target

# Fill ratio (should be close to 1.0)
dsql_reservoir_size / dsql_reservoir_target

# Checkout success rate
rate(dsql_reservoir_checkouts_total[5m]) / 
(rate(dsql_reservoir_checkouts_total[5m]) + rate(dsql_reservoir_empty_total[5m]))

# Discard breakdown
sum by (reason) (rate(dsql_reservoir_discards_total[5m]))

# Refill health
rate(dsql_reservoir_refills_total[5m])
sum by (reason) (rate(dsql_reservoir_refill_failures_total[5m]))
```

#### Recommended Alerts

```yaml
groups:
  - name: dsql-reservoir
    rules:
      - alert: DSQLReservoirLow
        expr: dsql_reservoir_size / dsql_reservoir_target < 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DSQL reservoir below 50% capacity"
          description: "Reservoir {{ $labels.service }} is at {{ $value | humanizePercentage }} capacity"

      - alert: DSQLReservoirEmpty
        expr: rate(dsql_reservoir_empty_total[5m]) > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "DSQL reservoir experiencing empty events"
          description: "{{ $labels.service }} reservoir is empty, causing connection retries"

      - alert: DSQLReservoirRefillFailures
        expr: rate(dsql_reservoir_refill_failures_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DSQL reservoir refill failures"
          description: "{{ $labels.service }} failing to create connections: {{ $labels.reason }}"

      - alert: DSQLGlobalLimitNearCapacity
        expr: dsql_distributed_conn_lease_active / dsql_distributed_conn_lease_limit > 0.9
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "DSQL global connection limit near capacity"
          description: "Cluster is at {{ $value | humanizePercentage }} of connection limit"
```

### Recovery Procedures

#### Procedure: Recover from Empty Reservoir

1. **Identify the cause** using the diagnostic steps above
2. **If rate limit issue:**
   - Stagger service restarts (don't restart all at once)
   - Temporarily reduce `DSQL_RESERVOIR_TARGET_READY`
3. **If IAM issue:**
   - Refresh credentials
   - Restart affected services
4. **If DSQL issue:**
   - Check AWS console for cluster health
   - Wait for cluster recovery, services will auto-recover

#### Procedure: Recover from Global Limit Exhaustion

1. **Check actual connection count** across all services
2. **If counter is accurate:**
   - Scale down services or reduce per-service pool size
   - Consider if all connections are necessary
3. **If counter is drifted:**
   - Wait for TTL cleanup (up to 3 minutes)
   - Or manually reset counter (see above)
4. **Prevent recurrence:**
   - Implement proper graceful shutdown
   - Monitor for service crashes

#### Procedure: Rolling Restart with Reservoir

When restarting services with reservoir mode:

1. **Restart one service at a time** to avoid rate limit contention
2. **Wait for reservoir to fill** before restarting next service
   - Watch for "Reservoir initial fill complete" log
   - Or monitor `dsql_reservoir_size` metric
3. **Allow 30-60 seconds between restarts** for rate limit budget recovery

### Configuration Tuning Guide

#### Symptom-Based Tuning

| Symptom | Parameter to Adjust | Direction |
|---------|---------------------|-----------|
| Empty reservoir events | `DSQL_RESERVOIR_TARGET_READY` | Increase |
| High discard rate (lifetime) | `DSQL_RESERVOIR_BASE_LIFETIME` | Increase |
| High discard rate (guard) | `DSQL_RESERVOIR_GUARD_WINDOW` | Decrease |
| Slow initial fill | Check rate limiter | - |
| Wasted connections | `DSQL_RESERVOIR_TARGET_READY` | Decrease |

#### Environment-Specific Recommendations

**Development (low traffic):**
```bash
DSQL_RESERVOIR_TARGET_READY=10
DSQL_RESERVOIR_LOW_WATERMARK=10
DSQL_RESERVOIR_BASE_LIFETIME=11m
DSQL_RESERVOIR_GUARD_WINDOW=30s
```

**Production (high traffic, single cluster):**
```bash
DSQL_RESERVOIR_TARGET_READY=50
DSQL_RESERVOIR_LOW_WATERMARK=50
DSQL_RESERVOIR_BASE_LIFETIME=11m
DSQL_RESERVOIR_GUARD_WINDOW=45s
```

**Production (high traffic, shared cluster):**
```bash
DSQL_RESERVOIR_TARGET_READY=50
DSQL_RESERVOIR_LOW_WATERMARK=50
DSQL_RESERVOIR_BASE_LIFETIME=11m
DSQL_RESERVOIR_GUARD_WINDOW=45s
DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
DSQL_DISTRIBUTED_CONN_LIMIT=10000
```

### FAQ

**Q: How long does initial fill take?**
A: Depends on rate limit and target size. With 100/sec limit and 50 target, expect ~1 second. With distributed rate limiting across many services, it may take longer.

**Q: What happens if DynamoDB is unavailable?**
A: Lease acquire fails, refiller backs off and retries. Existing connections continue to work. New connections cannot be created until DynamoDB recovers.

**Q: Can I disable reservoir mode without restarting?**
A: No, reservoir mode is determined at startup. You must restart the service with `DSQL_RESERVOIR_ENABLED=false`.

**Q: How do I know if reservoir mode is helping?**
A: Compare these metrics before/after:
- Connection wait time (`dsql_pool_wait_duration`)
- Empty pool events
- Latency during connection expiry windows

**Q: What's the overhead of reservoir mode?**
A: Minimal. The refiller runs in a single goroutine and only creates connections when needed. DynamoDB operations (if enabled) add ~5-10ms per connection create/destroy.
