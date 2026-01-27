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

5. **Brief Blocking Wait**: Before returning `ErrBadConn`, the driver waits briefly (100ms) for the refiller to catch up. This smooths out transient empty reservoir conditions.

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

DSQL has a hard limit of **10,000 concurrent connections per cluster**. When running multiple Temporal services (Frontend, History, Matching, Worker) across multiple instances, it's easy to exceed this limit without coordination. The distributed connection lease system provides cluster-wide coordination to prevent this.

### Why Global Limiting is Needed

Consider a typical production deployment:
- 4 Temporal services × 10 instances each = 40 service instances
- Each instance has 2 pools (default + visibility) × 50 connections = 100 connections per instance
- Total: 40 × 100 = 4,000 connections

Without coordination, scaling up or a burst of connection creation could easily exceed 10,000 connections. The lease system ensures the cluster-wide limit is respected.

### How It Works

The lease system uses a **two-item approach** in DynamoDB:

1. **Counter Item**: Tracks the current total connection count across all services
2. **Lease Items**: Individual records for each connection, enabling TTL-based cleanup

```
┌─────────────────────────────────────────────────────────────────┐
│                     DynamoDB Table                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Counter Item (1 per DSQL endpoint):                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ pk: "dsqllease_counter#cluster.dsql.us-east-1.on.aws"   │   │
│  │ active: 2847                                             │   │
│  │ updated_ms: 1706284800000                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Lease Items (1 per connection):                               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ pk: "dsqllease#cluster...#a1b2c3d4e5f6..."              │   │
│  │ ttl_epoch: 1706284980                                    │   │
│  │ service_name: "history"                                  │   │
│  │ created_ms: 1706284800000                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ pk: "dsqllease#cluster...#f6e5d4c3b2a1..."              │   │
│  │ ttl_epoch: 1706284990                                    │   │
│  │ service_name: "matching"                                 │   │
│  │ created_ms: 1706284810000                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ... (one item per active connection)                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### DynamoDB Schema

| Item Type | Partition Key Format | Attributes |
|-----------|---------------------|------------|
| Counter | `dsqllease_counter#<endpoint>` | `active` (Number), `updated_ms` (Number) |
| Lease | `dsqllease#<endpoint>#<leaseID>` | `ttl_epoch` (Number), `service_name` (String), `created_ms` (Number) |

**Key attributes:**
- `active`: Current count of active connections (on counter item)
- `ttl_epoch`: Unix timestamp for DynamoDB TTL auto-deletion (on lease items)
- `service_name`: Which Temporal service owns this lease (for debugging)
- `created_ms`: When the lease was created (for debugging)

### Acquire Operation

When the refiller wants to create a new connection, it first acquires a lease:

```go
func (l *DistributedConnLeases) Acquire(ctx context.Context) (string, error) {
    leaseID := generateRandomLeaseID()  // 32-char hex string
    
    // Atomic transaction: increment counter + create lease item
    _, err := l.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
        TransactItems: []types.TransactWriteItem{
            // 1. Increment counter (only if below limit)
            {
                Update: &types.Update{
                    TableName: aws.String(l.table),
                    Key: map[string]types.AttributeValue{
                        "pk": &types.AttributeValueMemberS{Value: "dsqllease_counter#" + l.endpoint},
                    },
                    UpdateExpression: aws.String("SET active = if_not_exists(active, :zero) + :one, updated_ms = :nowms"),
                    ConditionExpression: aws.String("attribute_not_exists(active) OR active < :limit"),
                    ExpressionAttributeValues: map[string]types.AttributeValue{
                        ":zero":  &types.AttributeValueMemberN{Value: "0"},
                        ":one":   &types.AttributeValueMemberN{Value: "1"},
                        ":limit": &types.AttributeValueMemberN{Value: "10000"},
                        ":nowms": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixMilli())},
                    },
                },
            },
            // 2. Create lease item with TTL
            {
                Put: &types.Put{
                    TableName: aws.String(l.table),
                    Item: map[string]types.AttributeValue{
                        "pk":           &types.AttributeValueMemberS{Value: "dsqllease#" + l.endpoint + "#" + leaseID},
                        "ttl_epoch":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().Add(3*time.Minute).Unix())},
                        "service_name": &types.AttributeValueMemberS{Value: l.serviceName},
                        "created_ms":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixMilli())},
                    },
                },
            },
        },
    })
    
    if err != nil {
        return "", err  // Limit reached or DynamoDB error
    }
    return leaseID, nil
}
```

**Key points:**
- Uses `TransactWriteItems` for atomicity - both operations succeed or both fail
- `ConditionExpression` ensures counter only increments if below limit
- If limit is reached, the transaction fails with `TransactionCanceledException`
- Lease ID is stored in `PhysicalConn.LeaseID` for later release

### Release Operation

When a connection is discarded (expired, bad, or reservoir full), the lease is released:

```go
func (l *DistributedConnLeases) Release(ctx context.Context, leaseID string) error {
    // Atomic transaction: delete lease item + decrement counter
    _, err := l.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
        TransactItems: []types.TransactWriteItem{
            // 1. Delete lease item
            {
                Delete: &types.Delete{
                    TableName: aws.String(l.table),
                    Key: map[string]types.AttributeValue{
                        "pk": &types.AttributeValueMemberS{Value: "dsqllease#" + l.endpoint + "#" + leaseID},
                    },
                },
            },
            // 2. Decrement counter
            {
                Update: &types.Update{
                    TableName: aws.String(l.table),
                    Key: map[string]types.AttributeValue{
                        "pk": &types.AttributeValueMemberS{Value: "dsqllease_counter#" + l.endpoint},
                    },
                    UpdateExpression: aws.String("SET active = active - :one, updated_ms = :nowms"),
                    ConditionExpression: aws.String("attribute_exists(active) AND active >= :one"),
                    ExpressionAttributeValues: map[string]types.AttributeValue{
                        ":one":   &types.AttributeValueMemberN{Value: "1"},
                        ":nowms": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixMilli())},
                    },
                },
            },
        },
    })
    
    return err  // Best-effort, errors are logged but not fatal
}
```

**Key points:**
- Release is best-effort - if it fails, TTL cleanup will eventually fix the counter
- `ConditionExpression` prevents counter from going negative
- Release is called from `Reservoir.discard()` when connections are discarded

### TTL-Based Cleanup

DynamoDB's TTL feature automatically deletes expired lease items. This handles crash recovery:

1. Service crashes without releasing leases
2. Lease items have `ttl_epoch` set to 3 minutes from creation
3. DynamoDB automatically deletes expired items (within ~48 hours, typically much faster)
4. Counter may temporarily be higher than actual connections

**Important**: TTL cleanup only deletes lease items, not the counter. This means the counter can drift if services crash frequently. In practice, this is acceptable because:
- TTL is short (3 minutes) so drift is bounded
- Counter eventually self-corrects as leases expire
- Operators can manually reset the counter if needed

### Sequence Diagram: Lease Acquire

```
Refiller                    DynamoDB                         DSQL
   │                           │                               │
   │──TransactWriteItems──────>│                               │
   │   (increment counter,     │                               │
   │    create lease item)     │                               │
   │                           │                               │
   │<──Success (leaseID)───────│                               │
   │                           │                               │
   │──Wait on rate limiter─────────────────────────────────────│
   │                           │                               │
   │──Open connection──────────────────────────────────────────>│
   │<──Connection established──────────────────────────────────│
   │                           │                               │
   │──Return(PhysicalConn{LeaseID: leaseID})──────────────────>│
   │                           │                               │
```

### Sequence Diagram: Lease Release (on discard)

```
Reservoir                   DynamoDB
   │                           │
   │  (connection expired or   │
   │   marked bad)             │
   │                           │
   │──Close physical conn──────│
   │                           │
   │──TransactWriteItems──────>│
   │   (delete lease item,     │
   │    decrement counter)     │
   │                           │
   │<──Success─────────────────│
   │                           │
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Enable global connection count limiting |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | DynamoDB table name (required if enabled) |
| `DSQL_DISTRIBUTED_CONN_LIMIT` | `10000` | Maximum connections cluster-wide |

### DynamoDB Table Setup

Create the DynamoDB table before enabling distributed leasing:

```bash
# Create table with on-demand billing
aws dynamodb create-table \
    --table-name temporal-dsql-conn-lease \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region us-east-1

# Wait for table to be active
aws dynamodb wait table-exists --table-name temporal-dsql-conn-lease --region us-east-1

# Enable TTL for automatic lease cleanup
aws dynamodb update-time-to-live \
    --table-name temporal-dsql-conn-lease \
    --time-to-live-specification Enabled=true,AttributeName=ttl_epoch \
    --region us-east-1
```

### IAM Permissions

Services need these DynamoDB permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:TransactWriteItems"
            ],
            "Resource": "arn:aws:dynamodb:us-east-1:123456789012:table/temporal-dsql-conn-lease"
        }
    ]
}
```

**Note**: Only `TransactWriteItems` is needed - no read permissions required.

### Failure Modes

| Scenario | Behavior |
|----------|----------|
| DynamoDB unavailable | Lease acquire fails, refiller backs off and retries |
| Limit reached | Lease acquire fails with `TransactionCanceledException`, refiller backs off |
| Service crash | Lease items expire via TTL (3 min), counter self-corrects |
| Network partition | Lease acquire may timeout, refiller retries |
| Counter drift | Bounded by TTL (3 min), self-corrects over time |

### Graceful Degradation

If DynamoDB is unavailable, the system degrades gracefully:
- Lease acquire fails, refiller backs off
- Existing connections continue to work
- New connections cannot be created until DynamoDB recovers
- No data loss or corruption

### Monitoring

Watch these indicators for lease system health:

```promql
# Lease acquire failures (should be 0 in steady state)
rate(dsql_reservoir_refill_failures_total{reason="lease_acquire"}[5m])

# If this is consistently > 0, either:
# - Global limit is reached (check counter in DynamoDB)
# - DynamoDB is having issues (check AWS health)
```

### Capacity Planning

When planning connection limits:

| Deployment | Services | Instances | Pools/Instance | Conns/Pool | Total | Headroom |
|------------|----------|-----------|----------------|------------|-------|----------|
| Small | 4 | 2 | 2 | 25 | 400 | 96% |
| Medium | 4 | 5 | 2 | 50 | 2,000 | 80% |
| Large | 4 | 10 | 2 | 50 | 4,000 | 60% |
| Very Large | 4 | 20 | 2 | 50 | 8,000 | 20% |

**Recommendation**: Keep total connections below 80% of the 10,000 limit to allow for burst capacity and rolling deployments.

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

#### Distributed Connection Lease Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Boolean | Enable DynamoDB-backed global connection count limiting. |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | String | DynamoDB table name for lease tracking. Required if distributed leasing is enabled. |
| `DSQL_DISTRIBUTED_CONN_LIMIT` | `10000` | Integer | Maximum connections allowed cluster-wide. Should match DSQL's connection limit. |

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
# Production configuration with distributed leasing
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=50
export DSQL_RESERVOIR_LOW_WATERMARK=50
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s

# Enable distributed connection leasing
export DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
export DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
export DSQL_DISTRIBUTED_CONN_LIMIT=10000
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

### Connection Checkout (Empty Reservoir with Wait)

```
database/sql          reservoirDriver          Reservoir           Refiller
     │                      │                      │                   │
     │──Open(dsn)──────────>│                      │                   │
     │                      │──TryCheckout(now)───>│                   │
     │                      │<──(nil, false)───────│                   │
     │                      │                      │                   │
     │                      │──WaitCheckout(100ms)>│                   │
     │                      │                      │<──Return(pc)──────│
     │                      │<──(PhysicalConn)─────│                   │
     │<──reservoirConn──────│                      │                   │
```

### Connection Checkout (Empty Reservoir, Timeout)

```
database/sql          reservoirDriver          Reservoir
     │                      │                      │
     │──Open(dsn)──────────>│                      │
     │                      │──TryCheckout(now)───>│
     │                      │<──(nil, false)───────│
     │                      │                      │
     │                      │──WaitCheckout(100ms)>│
     │                      │      (timeout)       │
     │                      │<──(nil, false)───────│
     │<──ErrBadConn─────────│                      │
     │                      │                      │
     │  (retry after backoff)                      │
```

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
| Reservoir empty | Brief wait, then return `ErrBadConn`, `database/sql` retries |
| Connection expired on checkout | Discard, return `ErrBadConn` |
| Connection error during use | Mark bad, discard on close |
| Rate limiter timeout | Refiller backs off, retries |
| Lease acquire fails (limit reached) | Refiller backs off, retries |
| DynamoDB unavailable | Fall back to local-only (no global limiting) |

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

## Implementation Status

### Completed

- [x] `reservoir.go` - Channel-based reservoir with guard window and ScanAndEvict
- [x] `reservoir_driver.go` - Driver implementation with brief blocking wait
- [x] `reservoir_conn.go` - Connection wrapper with bad connection tracking
- [x] `reservoir_refiller.go` - Continuous refiller with proactive expiry scanner
- [x] `distributed_conn_lease.go` - DynamoDB lease manager
- [x] `reservoir_config.go` - Configuration from environment
- [x] `conn_lease_config.go` - Lease configuration
- [x] Unit tests for all components
- [x] Initial fill synchronization
- [x] Connection age tracking (CreatedAt + Lifetime)
- [x] Proactive expiry scanning (every 1 second)
- [x] Eviction callback (lease release on discard)
- [x] Grafana dashboard panels
- [x] Metrics: checkout latency, refills, discards by reason

### Validated

- [x] Sub-millisecond checkout latency (p99 < 1ms)
- [x] Zero empty reservoir events under load
- [x] Continuous refill during expiry cycles
- [x] Proactive eviction of expiring connections

### Pending

- [ ] Integration tests with actual DSQL cluster at scale
- [ ] Setup script for DynamoDB table
- [ ] ECS task definition updates

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

#### Issue: Empty Reservoir Events (ErrBadConn)

**Symptoms:**
- `dsql_reservoir_empty_total` counter increasing
- Application logs showing connection retry messages
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
