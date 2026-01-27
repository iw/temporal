# DSQL Implementation Details

This document covers the technical implementation of Aurora DSQL support in Temporal, including schema changes, OCC handling, and the major contributions to achieve DSQL compatibility.

## Table of Contents

- [Code Structure](#code-structure)
- [Schema Changes](#schema-changes)
  - [BYTEA to UUID in Composite Primary Keys](#bytea-to-uuid-in-composite-primary-keys)
  - [BIGSERIAL Removal](#bigserial-removal)
  - [Other Schema Changes](#other-schema-changes)
- [FOR UPDATE and Locking Changes](#for-update-and-locking-changes)
  - [FOR UPDATE on Single Table Only](#for-update-on-single-table-only)
  - [FOR SHARE Not Supported](#for-share-not-supported)
- [Optimistic Concurrency Control (OCC)](#optimistic-concurrency-control-occ)
  - [Retry Logic](#retry-logic-retrygo)
  - [Error Classification](#error-classification-errorsgo)
  - [Transaction Retry Policy](#transaction-retry-policy-tx_retry_policygo)
- [Compare-And-Swap (CAS) Updates](#compare-and-swap-cas-updates)
  - [Shard Updates](#shard-updates-shardgo-cas_updatesgo)
  - [Auto-Fenced Updates](#auto-fenced-updates)
  - [Generic CAS Helper](#generic-cas-helper)
- [IAM Authentication](#iam-authentication)
  - [Token-Refreshing Driver](#token-refreshing-driver-drivertokenrefreshinggo)
  - [Token Cache](#token-cache-tokencachego)
- [Connection Rate Limiting](#connection-rate-limiting)
- [Pool Metrics Collection](#pool-metrics-collection)
- [Connection Pool Pre-Warming](#connection-pool-pre-warming)
- [Pool Keeper](#pool-keeper)
- [Connection Reservoir](#connection-reservoir)
  - [Reservoir Architecture](#reservoir-architecture)
  - [Reservoir Configuration](#reservoir-configuration)
  - [Reservoir Driver](#reservoir-driver)
  - [Reservoir Refiller](#reservoir-refiller)
  - [Distributed Connection Leasing](#distributed-connection-leasing)
- [Pool Simulator](#pool-simulator)
- [Testing](#testing)

## Code Structure

```
common/persistence/sql/sqlplugin/dsql/
├── plugin.go                      # Plugin registration, IAM auth, token refresh
├── db.go                          # Database handle, transaction management
├── execution.go                   # Workflow execution operations
├── shard.go                       # Shard management with CAS updates
├── cas_updates.go                 # Compare-And-Swap update patterns
├── fenced_updates.go              # Fenced update helpers
├── retry.go                       # OCC retry logic with backoff
├── tx_retry_policy.go             # Transaction-level retry policy
├── errors.go                      # Error classification (retryable vs permanent)
├── metrics.go                     # DSQL-specific metrics
├── token_cache.go                 # IAM token caching
├── connection_rate_limiter.go     # Local (per-instance) rate limiting
├── distributed_rate_limiter.go    # DynamoDB-backed distributed rate limiting
├── reservoir_config.go            # Reservoir configuration from environment
├── conn_lease_config.go           # Connection lease configuration
├── distributed_conn_lease.go      # DynamoDB-backed global connection count limiting
├── pool_warmup.go                 # Connection pool pre-warming at startup
├── pool_keeper.go                 # Background pool maintenance
├── uuid.go                        # UUID string conversion utilities
├── typeconv.go                    # DateTime conversion
├── driver/
│   ├── token_refreshing.go        # Token-refreshing database driver
│   ├── connection_registry.go     # Connection tracking for lifecycle management
│   ├── reservoir.go               # Connection reservoir (channel-based buffer)
│   ├── reservoir_driver.go        # Reservoir-backed driver implementation
│   ├── reservoir_conn.go          # Connection wrapper for reservoir
│   └── reservoir_refiller.go      # Background refiller goroutine
└── session/
    └── session.go                 # Connection session management

tools/poolsim/                     # Discrete event simulator for reservoir behavior
├── main.go                        # Entry point and simulation runner
├── config.go                      # Configuration loading and defaults
├── reservoir.go                   # Connection reservoir model
├── refiller.go                    # Continuous refiller simulation
├── workload.go                    # Database workload simulation
├── limiter.go                     # Rate limiter model
├── metrics.go                     # Metrics collection
├── assertions.go                  # Validation assertions
├── scenario.go                    # Scenario configuration (cold_start, mass_drop, etc.)
├── service.go                     # Service abstraction
├── sim.go                         # Discrete event simulation engine
├── README.md                      # Documentation
└── scenarios/                     # Pre-configured test scenarios
    ├── local.yaml                 # Local development (200 connections)
    ├── ecs-150wps.yaml            # ECS 150 WPS (2,000 connections)
    ├── ecs-400wps.yaml            # ECS 400 WPS (22,000 connections)
    ├── mass-drop.yaml             # Mass connection drop recovery
    └── workload-test.yaml         # Workload simulation test

schema/dsql/v12/temporal/
└── schema.sql                     # DSQL-compatible schema
```

## Schema Changes

### BYTEA to UUID in Composite Primary Keys

DSQL has limitations with BYTEA columns in composite primary keys. All identifier columns were changed from `BYTEA` to `UUID`:

```sql
-- PostgreSQL (original)
CREATE TABLE executions(
  namespace_id BYTEA NOT NULL,
  run_id BYTEA NOT NULL,
  ...
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

-- DSQL (modified)
CREATE TABLE executions(
  namespace_id UUID NOT NULL,
  run_id UUID NOT NULL,
  ...
  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);
```

**Tables affected:**
- `executions`, `current_executions`, `buffered_events`
- `tasks`, `task_queues`, `tasks_v2`, `task_queues_v2`
- `activity_info_maps`, `timer_info_maps`, `child_execution_info_maps`
- `request_cancel_info_maps`, `signal_info_maps`, `signals_requested_sets`
- `history_node`, `history_tree`, `cluster_membership`
- `namespaces`, `nexus_endpoints`, `build_id_to_task_queue`

**Application-level handling** (`uuid.go`):

```go
// Convert primitives.UUID to string for DSQL queries
func UUIDToString(u primitives.UUID) string {
    return formatUUID(u) // Returns "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}

// Usage in execution.go
namespaceIDStr := row.NamespaceID.String()
runIDStr := row.RunID.String()
```

### BIGSERIAL Removal

DSQL doesn't support auto-increment columns. The `buffered_events.id` column was changed from `BIGSERIAL` to `BIGINT` with application-level ID generation:

```sql
-- PostgreSQL
id BIGSERIAL NOT NULL

-- DSQL
id BIGINT NOT NULL
```

**Snowflake ID Generator** (`idgenerator.go`):

```go
type SnowflakeIDGenerator struct {
    nodeID     int64  // 10 bits: 0-1023
    sequence   int64  // 12 bits: 0-4095
    lastTime   int64  // 41 bits: milliseconds
}

// Generates 4096 unique IDs per millisecond per node
func (g *SnowflakeIDGenerator) NextID() (int64, error)
```

### Other Schema Changes

| Change | Reason |
|--------|--------|
| `CHECK` constraints removed | DSQL doesn't support CHECK |
| Complex `DEFAULT` removed | Limited DEFAULT expression support |
| `UNIQUE` → separate indexes | Better DSQL compatibility |
| `CREATE INDEX ASYNC` | Non-blocking index creation |

## FOR UPDATE and Locking Changes

DSQL has significant limitations with locking clauses that required major changes.

### FOR UPDATE on Single Table Only

DSQL only allows `FOR UPDATE` on a single base table per statement. JOINs with `FOR UPDATE` fail with `SQLSTATE 0A000`.

**Problem:** `LockCurrentExecutionsJoinExecutions` used a JOIN with FOR UPDATE.

**Solution:** Split into two queries:

```go
// execution.go - LockCurrentExecutionsJoinExecutions
func (pdb *db) LockCurrentExecutionsJoinExecutions(ctx context.Context, filter ...) {
    // 1) Lock current_executions (single-table FOR UPDATE)
    err := pdb.GetContext(ctx, &row, lockCurrentExecutionQuery, ...)
    
    // 2) Read executions.last_write_version separately (no FOR UPDATE)
    err := pdb.GetContext(ctx, &lastWriteVersion, getExecutionLastWriteVersionQuery, ...)
    
    row.LastWriteVersion = lastWriteVersion
    return []sqlplugin.CurrentExecutionsRow{row}, nil
}
```

### FOR SHARE Not Supported

DSQL doesn't support `FOR SHARE` (read locks). All read-lock methods delegate to write-lock or use optimistic reads.

**ReadLockExecutions** (`execution.go`):

```go
// DSQL doesn't support FOR SHARE, delegate to WriteLockExecutions
// Safe because: method is unused in codebase, WriteLockExecutions provides
// stronger guarantees
func (pdb *db) ReadLockExecutions(ctx context.Context, filter ...) (int64, int64, error) {
    return pdb.WriteLockExecutions(ctx, filter)
}
```

**ReadLockShards** (`shard.go`):

```go
// DSQL-compatible query without FOR SHARE
const dsqlReadLockShardQry = `SELECT range_id FROM shards WHERE shard_id = $1`

func (pdb *db) ReadLockShards(ctx context.Context, filter ...) (int64, error) {
    // Use retry manager for OCC handling
    if pdb.tx == nil && pdb.retryManager != nil {
        result, err := pdb.retryManager.RunTx(ctx, "ReadLockShards", func(tx *sql.Tx) (interface{}, error) {
            var rangeID int64
            err := tx.QueryRowContext(ctx, dsqlReadLockShardQry, filter.ShardID).Scan(&rangeID)
            return rangeID, err
        })
        return result.(int64), err
    }
    // Direct execution for transaction contexts
    return pdb.GetContext(ctx, &rangeID, dsqlReadLockShardQry, filter.ShardID)
}
```

## Optimistic Concurrency Control (OCC)

DSQL uses OCC instead of pessimistic locking. Concurrent transactions that conflict receive `SQLSTATE 40001` (serialization failure).

### Retry Logic (`retry.go`)

```go
type RetryConfig struct {
    MaxRetries   int           // Default: 5
    BaseDelay    time.Duration // Default: 100ms
    MaxDelay     time.Duration // Default: 5s
    JitterFactor float64       // Default: 0.25
}

func (r *RetryManager) RunTx(ctx context.Context, op string, 
    fn func(*sql.Tx) (interface{}, error)) (interface{}, error) {
    
    for attempt := 1; attempt <= maxAttempts; attempt++ {
        tx, err := r.db.BeginTx(ctx, nil)
        if err != nil {
            return nil, err
        }
        
        result, err := fn(tx)
        if err != nil {
            tx.Rollback()
            
            cls := classifyError(err)
            r.metrics.IncTxErrorClass(op, cls.String())
            
            if cls == ErrorTypeRetryable && attempt < maxAttempts {
                r.metrics.IncTxConflict(op)
                r.metrics.IncTxRetry(op, attempt)
                
                delay := r.calculateBackoff(attempt - 1)
                r.metrics.ObserveTxBackoff(op, delay)
                time.Sleep(delay)
                continue
            }
            return nil, err
        }
        
        if err := tx.Commit(); err != nil {
            // Handle commit-time conflicts
            if isRetryable(err) && attempt < maxAttempts {
                delay := r.calculateBackoff(attempt - 1)
                time.Sleep(delay)
                continue
            }
            return nil, err
        }
        
        return result, nil
    }
    
    r.metrics.IncTxExhausted(op)
    return nil, fmt.Errorf("max retries exceeded")
}
```

### Error Classification (`errors.go`)

```go
type ErrorType int

const (
    ErrorTypeUnknown ErrorType = iota
    ErrorTypeRetryable          // SQLSTATE 40001 - retry
    ErrorTypeConditionFailed    // CAS failure - don't retry
    ErrorTypePermanent          // Other errors - don't retry
    ErrorTypeUnsupportedFeature // SQLSTATE 0A000
)

func classifyError(err error) ErrorType {
    var pgErr *pgconn.PgError
    if errors.As(err, &pgErr) {
        switch pgErr.SQLState() {
        case "40001":
            return ErrorTypeRetryable
        case "0A000":
            return ErrorTypeUnsupportedFeature
        }
    }
    
    if IsConditionFailedError(err) {
        return ErrorTypeConditionFailed
    }
    
    return ErrorTypePermanent
}
```

### Transaction Retry Policy (`tx_retry_policy.go`)

Exposes retry policy to Temporal's SqlStore for transaction-boundary retries:

```go
func (pdb *db) TxRetryPolicy() persistsql.TxRetryPolicy {
    if pdb.retryManager == nil {
        return nil
    }
    return NewDSQLTxRetryPolicy(pdb.retryManager)
}
```

## Compare-And-Swap (CAS) Updates

Traditional locking is replaced with conditional updates using fencing tokens.

### Shard Updates (`shard.go`, `cas_updates.go`)

```go
// UpdateShardsWithFencing performs a fenced update using range_id
func (pdb *db) UpdateShardsWithFencing(ctx context.Context, row *sqlplugin.ShardsRow, 
    expectedRangeID int64) (sql.Result, error) {
    
    const query = `UPDATE shards 
        SET range_id = $1, data = $2, data_encoding = $3 
        WHERE shard_id = $4 AND range_id = $5`

    result, err := pdb.ExecContext(ctx, query,
        row.RangeID, row.Data, row.DataEncoding, row.ShardID, expectedRangeID)
    if err != nil {
        return nil, err
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return nil, NewConditionFailedError(ConditionFailedShard,
            fmt.Sprintf("shard %d range_id changed from expected %d", 
                row.ShardID, expectedRangeID))
    }

    return result, nil
}
```

### Auto-Fenced Updates

For non-transaction contexts, automatically read current fencing token and apply CAS:

```go
func (pdb *db) UpdateShardsAutoFenced(ctx context.Context, row *sqlplugin.ShardsRow) (sql.Result, error) {
    // Read current range_id
    var currentRangeID int64
    err := pdb.GetContext(ctx, &currentRangeID, 
        `SELECT range_id FROM shards WHERE shard_id = $1`, row.ShardID)
    if err != nil {
        return nil, err
    }

    // Fenced update with current range_id as expected value
    return pdb.UpdateShardsWithFencing(ctx, row, currentRangeID)
}
```

### Generic CAS Helper

```go
func (pdb *db) GenericCASUpdate(ctx context.Context, query string, args []interface{},
    entityDesc string, fencingToken interface{}) error {
    
    result, err := pdb.ExecContext(ctx, query, args...)
    if err != nil {
        return err
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        return NewConditionFailedError(ConditionFailedUnknown,
            fmt.Sprintf("%s fencing token changed from expected %v", 
                entityDesc, fencingToken))
    }

    return nil
}
```

## IAM Authentication

### Token-Refreshing Driver (`driver/token_refreshing.go`)

Custom database driver that injects fresh IAM tokens for each new connection:

```go
// Token provider called for each new connection
tokenProvider := func(ctx context.Context) (string, error) {
    return tokenCache.GetToken(ctx, endpoint, region, user, duration)
}

// Register driver with token provider
driverName, err := driver.RegisterTokenRefreshingDriverWithLogger(
    adminUser, tokenProvider, logFunc)

// Open connection - driver calls tokenProvider automatically
db, err := sql.Open(driverName, baseDSN)
```

### Token Cache (`token_cache.go`)

```go
type TokenCache struct {
    credentials aws.CredentialsProvider
    cache       sync.Map  // endpoint -> cachedToken
}

type cachedToken struct {
    token     string
    expiresAt time.Time
}

func (tc *TokenCache) GetToken(ctx context.Context, endpoint, region, user string, 
    duration time.Duration) (string, error) {
    // Check cache first
    if cached, ok := tc.cache.Load(endpoint); ok {
        if time.Now().Before(cached.expiresAt) {
            return cached.token, nil
        }
    }
    
    // Generate new token
    token, err := dsql.GenerateDbConnectAdminAuthToken(ctx, endpoint, region, 
        tc.credentials, duration)
    if err != nil {
        return "", err
    }
    
    // Cache with expiry
    tc.cache.Store(endpoint, &cachedToken{
        token:     token,
        expiresAt: time.Now().Add(duration - 1*time.Minute), // Buffer
    })
    
    return token, nil
}
```

## Connection Rate Limiting

DSQL has cluster-wide connection limits (100 new connections/sec, 1000 burst, 10000 max). The plugin provides two rate limiting modes.

**Important**: Rate limiting only applies to NEW connection establishment (TCP/TLS handshake + IAM authentication), not to queries. Once a connection is in the pool, queries flow through without rate limiting.

### Local Rate Limiting (Default)

Per-instance rate limiting using Go's `rate.Limiter`:

```go
type ConnectionRateLimiter struct {
    limiter *rate.Limiter
}

func NewConnectionRateLimiter() *ConnectionRateLimiter {
    rateLimit := getEnvInt("DSQL_CONNECTION_RATE_LIMIT", 10)
    burstLimit := getEnvInt("DSQL_CONNECTION_BURST_LIMIT", 100)
    
    return &ConnectionRateLimiter{
        limiter: rate.NewLimiter(rate.Limit(rateLimit), burstLimit),
    }
}

// Staggered startup to prevent thundering herd
func StaggeredStartupDelay() time.Duration {
    if !getEnvBool("DSQL_STAGGERED_STARTUP", true) {
        return 0
    }
    maxDelay := getEnvDuration("DSQL_STAGGERED_STARTUP_MAX_DELAY", 5*time.Second)
    return time.Duration(rand.Int63n(int64(maxDelay)))
}
```

### Distributed Rate Limiting (Recommended for Production)

DynamoDB-backed coordination across all service instances:

```go
type DistributedRateLimiter struct {
    ddb            *dynamodb.Client
    tableName      string
    endpoint       string
    LimitPerSecond int64         // Default: 100 (DSQL cluster limit)
    MaxWait        time.Duration // Default: 30s
    BackoffBase    time.Duration // Default: 25ms
}

// Wait blocks until a connection permit can be acquired
func (l *DistributedRateLimiter) Wait(ctx context.Context) error {
    return l.Acquire(ctx, 1)
}

// Acquire reserves n permits for the current second
func (l *DistributedRateLimiter) Acquire(ctx context.Context, n int64) error {
    deadline := time.Now().Add(l.MaxWait)
    
    for {
        sec := time.Now().UTC().Unix()
        ok, err := l.tryAcquireOnce(ctx, sec, n)
        if ok {
            return nil
        }
        
        if time.Now().After(deadline) {
            return fmt.Errorf("timeout acquiring permit")
        }
        
        // Jittered backoff, wait for next second boundary if close
        time.Sleep(l.jitteredBackoff())
    }
}

// tryAcquireOnce attempts atomic increment with conditional check
func (l *DistributedRateLimiter) tryAcquireOnce(ctx context.Context, sec, n int64) (bool, error) {
    pk := fmt.Sprintf("dsqlconnect#%s#%d", l.endpoint, sec)
    
    _, err := l.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
        TableName: aws.String(l.tableName),
        Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}},
        UpdateExpression: aws.String("SET updated_at_ms = :nowms, ttl_epoch = :ttl ADD #cnt :n"),
        ExpressionAttributeNames: map[string]string{"#cnt": "count"},
        ExpressionAttributeValues: map[string]types.AttributeValue{
            ":n":           &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", n)},
            ":limitMinusN": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", l.LimitPerSecond-n)},
            // ... other values
        },
        // Allow if: item is new OR current count <= (limit - n)
        ConditionExpression: aws.String("attribute_not_exists(#cnt) OR #cnt <= :limitMinusN"),
    })
    
    if isConditionalFail(err) {
        return false, nil // Limit exceeded, retry
    }
    return err == nil, err
}
```

**DynamoDB Table Schema:**
- Partition key: `pk` (String) - Format: `dsqlconnect#<endpoint>#<unix_second>`
- TTL attribute: `ttl_epoch` (Number) - Auto-cleanup after 3 minutes

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED` | `false` | Enable distributed mode |
| `DSQL_DISTRIBUTED_RATE_LIMITER_TABLE` | - | DynamoDB table name |
| `DSQL_DISTRIBUTED_RATE_LIMITER_LIMIT` | `100` | Cluster-wide limit/sec |
| `DSQL_DISTRIBUTED_RATE_LIMITER_MAX_WAIT` | `30s` | Max wait for permit |

### Rate Limiter Integration

The rate limiter is integrated into the token-refreshing driver's `Open()` method, ensuring ALL connection attempts are rate-limited:

```go
func (d *tokenRefreshingDriver) Open(dsn string) (driver.Conn, error) {
    // Apply rate limiting BEFORE attempting connection
    if d.rateLimiter != nil {
        if err := d.rateLimiter.Wait(ctx); err != nil {
            return nil, fmt.Errorf("connection rate limit exceeded: %w", err)
        }
    }
    
    // Get fresh token and open connection
    token, err := d.tokenProvider(ctx)
    // ...
}
```

This ensures rate limiting applies to:
- Initial pool creation
- Pool growth under load (`database/sql` internal connections)
- Connection replacement after `MaxConnLifetime` expiry
- Reconnection after connection failures

## Pool Metrics Collection

Background goroutine samples connection pool statistics:

```go
func (m *dsqlMetricsImpl) StartPoolCollector(db *sql.DB, interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                stats := db.Stats()
                m.poolMaxOpen.Record(float64(stats.MaxOpenConnections))
                m.poolOpen.Record(float64(stats.OpenConnections))
                m.poolInUse.Record(float64(stats.InUse))
                m.poolIdle.Record(float64(stats.Idle))
                // ... wait count and duration
            }
        }
    }()
}
```

## Connection Pool Pre-Warming

DSQL has a cluster-wide connection rate limit of 100 connections/second. To avoid connection creation under load, the pool is pre-warmed at startup to its maximum size.

### Why Pre-Warming is Critical

1. **Rate Limit Pressure**: If the pool starts empty and grows on-demand, multiple services competing for connections can exhaust the 100/sec budget
2. **Cold-Start Latency**: First requests would wait for connection establishment (TCP + TLS + IAM auth)
3. **Cascade Failures**: Under load, connection timeouts can cascade as services retry

### Pool Configuration

```go
const (
    // Pool MUST stay at max size to avoid connection creation under load
    DefaultMaxConns     = 100
    DefaultMaxIdleConns = 100  // MUST equal MaxConns
    
    // CRITICAL: Must be 0 to prevent pool decay
    // Go's database/sql closes idle connections after this timeout
    DefaultMaxConnIdleTime = 0
    
    // 55 minutes, safely under DSQL's 60 minute limit
    DefaultMaxConnLifetime = 55 * time.Minute
)
```

### Warmup Implementation (`pool_warmup.go`)

```go
type PoolWarmupConfig struct {
    TargetConnections int           // Default: matches MaxConns (100)
    MaxRetries        int           // Default: 5
    RetryBackoff      time.Duration // Default: 200ms (with jitter)
    MaxBackoff        time.Duration // Default: 5s
    ConnectionTimeout time.Duration // Default: 10s per connection
}

func WarmupPool(ctx context.Context, db *sql.DB, cfg PoolWarmupConfig, 
    logger log.Logger) error {
    
    current := db.Stats().OpenConnections
    toCreate := cfg.TargetConnections - current
    
    logger.Info("Starting DSQL pool warmup",
        tag.NewInt("current_connections", current),
        tag.NewInt("target_connections", cfg.TargetConnections),
        tag.NewInt("connections_to_create", toCreate))
    
    var created, failed int
    for i := 0; i < toCreate; i++ {
        err := createOneConnection(ctx, db, cfg)
        if err != nil {
            failed++
            logger.Warn("Pool warmup connection failed", tag.Error(err))
        } else {
            created++
        }
    }
    
    logger.Info("DSQL pool warmup complete",
        tag.NewInt("connections_created", created),
        tag.NewInt("connections_failed", failed),
        tag.NewInt("final_open_connections", db.Stats().OpenConnections))
    
    return nil
}

func createOneConnection(ctx context.Context, db *sql.DB, cfg PoolWarmupConfig) error {
    // Each connection has its own timeout and retry logic
    for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
        connCtx, cancel := context.WithTimeout(ctx, cfg.ConnectionTimeout)
        conn, err := db.Conn(connCtx)
        cancel()
        
        if err == nil {
            // Ping to ensure connection is valid, then return to pool
            err = conn.PingContext(ctx)
            conn.Close() // Returns to pool, doesn't close
            if err == nil {
                return nil
            }
        }
        
        if attempt < cfg.MaxRetries {
            backoff := calculateBackoffWithJitter(cfg, attempt)
            time.Sleep(backoff)
        }
    }
    return fmt.Errorf("failed after %d attempts", cfg.MaxRetries)
}
```

### Warmup Behavior

- **Sequential creation**: Connections are created one at a time (not in batches) for reliability
- **Per-connection timeout**: Each connection has a 10-second timeout
- **Exponential backoff**: Failed connections retry with jitter (50-150%)
- **Best-effort**: Warmup failures are logged but don't prevent startup
- **Synchronous**: Warmup completes before the connection is returned to callers

### Startup Logs

On successful warmup, you'll see:

```
DSQL connection pool configured  max_open_conns=100 current_open=1 max_conn_lifetime=55m0s max_conn_idle_time=0s
Starting DSQL connection pool warmup
Starting DSQL pool warmup  current_connections=1 target_connections=100 connections_to_create=99
DSQL pool warmup complete  connections_created=99 connections_failed=0 final_open_connections=100
```

## Pool Keeper

After warmup, the Pool Keeper maintains pool size by replacing connections closed by `MaxConnLifetime`.

### Why Pool Keeper is Needed

Go's `database/sql` closes connections after `MaxConnLifetime` (55 minutes). Without active maintenance, the pool would gradually shrink as connections age out. The Pool Keeper detects deficits and creates replacement connections.

### Ratio-Based Batching

The Pool Keeper uses ratio-based batching to handle peak expiry rates:

```go
// MaxConnsPerTick = ceil(poolSize / staggerSeconds), capped at 10
// Examples:
//   - 50 connections:  ceil(50/120)  = 1 per tick
//   - 100 connections: ceil(100/120) = 1 per tick  
//   - 500 connections: ceil(500/120) = 5 per tick
//   - 1000+ connections: capped at 10 per tick
```

**Important**: `MaxConnsPerTick` is a LOCAL desire, not a guarantee. The global rate limiter (100 conn/sec cluster-wide) is the HARD constraint. When multiple pools compete for the rate limit budget, they back off and retry.

### Implementation (`pool_keeper.go`)

```go
type PoolKeeperConfig struct {
    TargetPoolSize    int           // Desired pool size
    TickInterval      time.Duration // Default: 1 second
    MaxConnsPerTick   int           // Calculated from pool size
    ConnectionTimeout time.Duration // Default: 10 seconds
}

func (pk *PoolKeeper) tick(ctx context.Context) {
    stats := pk.db.Stats()
    deficit := pk.cfg.TargetPoolSize - stats.OpenConnections
    
    if deficit <= 0 {
        return // Pool is at or above target
    }
    
    // Create at most MaxConnsPerTick connections this tick
    toCreate := min(deficit, pk.cfg.MaxConnsPerTick)
    
    for i := 0; i < toCreate; i++ {
        pk.createOneConnection(ctx)
    }
}
```

### Startup Logs

```
DSQL pool keeper started  target_pool_size=100 tick_interval=1s max_conns_per_tick=1
```

## Connection Reservoir

The Connection Reservoir is an advanced connection management mode that addresses the fundamental mismatch between DSQL's cluster-wide connection rate limit (100 connections/second) and the bursty nature of connection pool refill when connections expire due to `MaxConnLifetime`.

### Problem Statement

When connections expire due to `MaxConnLifetime`, the pool needs to replace them. If many connections expire at once (burst expiry), the pool shrinks because the refill rate can't keep up with the rate limit. The reservoir solves this by maintaining a buffer of pre-created connections that can be instantly handed out without waiting for rate limiters.

### Reservoir Architecture

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

### Key Components

#### Reservoir (`driver/reservoir.go`)

The reservoir is a channel-based buffer of physical connections:

```go
type Reservoir struct {
    ready       chan *PhysicalConn  // Buffered channel
    guardWindow time.Duration       // Discard if remaining lifetime within this window
    leaseRel    LeaseReleaser       // For releasing global leases
    metrics     ReservoirMetrics
}

type PhysicalConn struct {
    Conn      driver.Conn
    CreatedAt time.Time     // When connection was established
    Lifetime  time.Duration // Total lifetime (base + jitter)
    LeaseID   string        // For global connection count tracking
}
```

**Key Operations:**

- `TryCheckout(now)` - Non-blocking checkout from channel
- `WaitCheckout(timeout)` - Brief blocking wait for transient empty conditions
- `Return(pc, now)` - Non-blocking return to channel (discard if full or expired)

**Design Decisions:**

1. **Channel-based**: Using a buffered channel provides natural FIFO ordering and thread-safe access without explicit locking for the hot path.

2. **Guard Window**: Connections within `guardWindow` of expiry are discarded on checkout/return. This prevents handing out connections that will expire mid-transaction.

3. **Non-blocking Return**: If the channel is full, returned connections are discarded. This prevents blocking the caller.

4. **Age Tracking**: Connection age is tracked from creation time rather than using a fixed expiry time. This allows computing remaining lifetime at checkout, which is more accurate when connections sit in the reservoir for varying amounts of time.

### Reservoir Configuration

The reservoir is configured entirely through environment variables, allowing operators to tune behavior without code changes.

#### Core Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_RESERVOIR_ENABLED` | `false` | Boolean | Enable reservoir mode. When `false`, uses standard token-refreshing driver. |
| `DSQL_RESERVOIR_TARGET_READY` | `maxOpen` | Integer | Target number of connections to maintain in the reservoir. |
| `DSQL_RESERVOIR_LOW_WATERMARK` | `maxOpen` | Integer | Threshold below which the refiller uses aggressive pacing. |
| `DSQL_RESERVOIR_BASE_LIFETIME` | `11m` | Duration | Base lifetime for connections before they are discarded. |
| `DSQL_RESERVOIR_LIFETIME_JITTER` | `2m` | Duration | Random jitter added to each connection's lifetime. |
| `DSQL_RESERVOIR_GUARD_WINDOW` | `45s` | Duration | Time before expiry when connections are considered too old. |

#### Distributed Connection Lease Configuration

| Variable | Default | Type | Description |
|----------|---------|------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Boolean | Enable DynamoDB-backed global connection count limiting. |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | String | DynamoDB table name for lease tracking. |
| `DSQL_DISTRIBUTED_CONN_LIMIT` | `10000` | Integer | Maximum connections allowed cluster-wide. |

#### Configuration Example

```bash
# Enable reservoir mode
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=50
export DSQL_RESERVOIR_LOW_WATERMARK=50
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s

# Optional: Enable distributed connection leasing
export DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
export DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
export DSQL_DISTRIBUTED_CONN_LIMIT=10000
```

For detailed configuration guidance, see [Reservoir Design - Configuration](reservoir-design.md#configuration).

### Reservoir Driver

The reservoir driver implements `driver.Driver` and sources connections from the reservoir:

```go
func (d *reservoirDriver) Open(_ string) (driver.Conn, error) {
    now := time.Now().UTC()
    
    // Try non-blocking first
    if pc, ok := d.res.TryCheckout(now); ok {
        return newReservoirConn(d.res, pc), nil
    }
    
    // Brief wait for refiller to catch up (100ms default)
    if pc, ok := d.res.WaitCheckout(d.emptyWaitTimeout); ok {
        return newReservoirConn(d.res, pc), nil
    }
    
    // Reservoir empty - return ErrBadConn to trigger retry
    return nil, driver.ErrBadConn
}
```

**Design Decisions:**

1. **ErrBadConn on Empty**: When reservoir is empty, returning `driver.ErrBadConn` tells `database/sql` to retry. This is the standard mechanism for transient connection failures.

2. **Brief Blocking Wait**: Before returning `ErrBadConn`, the driver waits briefly (100ms) for the refiller to catch up. This smooths out transient empty reservoir conditions.

3. **DSN Ignored**: The DSN parameter is ignored because connections are pre-created by the refiller.

### Reservoir Refiller

The refiller is a background goroutine that continuously fills the reservoir:

```go
func (r *reservoirRefiller) loop() {
    steadyInterval := r.steadyStateInterval()
    
    for {
        ready := r.res.Len()
        need := r.cfg.TargetReady - ready
        
        if need <= 0 {
            // At target - check again after steady state interval
            sleepOrStop(r.stopC, steadyInterval)
            continue
        }
        
        // Create one connection
        r.openOne(ctx)
        
        // Calculate interval with smooth transition
        interval := r.calculateRefillInterval(ready, WarmupInterval, steadyInterval)
        sleepOrStop(r.stopC, interval)
    }
}
```

**Refiller Pacing:**

The refiller uses two modes with smooth transition:

1. **Warmup Mode** (below low watermark): Uses full rate limit budget with minimal delay (10ms) between attempts. The rate limiter controls actual pacing.

2. **Steady State Mode** (at or above target): Uses minimal rate limit budget by pacing based on connection lifetime. Only creates connections as fast as they expire.

3. **Transition Zone** (between low watermark and target): Smoothly interpolates between warmup and steady-state intervals.

**Steady State Interval Calculation:**

```go
// With N connections and lifetime L, need to replace N/L connections per second.
// Example: 50 connections with 11min lifetime = 50/(11*60) = 0.076/sec = 1 every ~13 seconds
interval := time.Duration(float64(baseLifetime) / float64(targetReady))
```

### Distributed Connection Leasing

The reservoir can optionally use DynamoDB-backed global connection count limiting to enforce DSQL's connection limit (default 10,000, can be raised) across all services.

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Enable global conn limiting |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | DynamoDB table name |
| `DSQL_DISTRIBUTED_CONN_LIMIT` | `10000` | Global connection limit |

**DynamoDB Schema:**

| Item Type | Partition Key | Attributes |
|-----------|---------------|------------|
| Counter | `dsqllease_counter#<endpoint>` | `active` (count), `updated_ms` |
| Lease | `dsqllease#<endpoint>#<leaseID>` | `ttl_epoch`, `service_name`, `created_ms` |

**Operations:**

- `Acquire(ctx)` - TransactWriteItems: increment counter (if < limit) + put lease item
- `Release(ctx, leaseID)` - TransactWriteItems: delete lease item + decrement counter

**Design Decisions:**

1. **Two-Item Approach**: Counter item for fast limit checking, lease items for TTL cleanup. This allows atomic acquire/release while enabling automatic cleanup of crashed services.

2. **TTL Cleanup**: Lease items have TTL (3 minutes). If a service crashes, its leases are automatically cleaned up.

3. **Conditional Update**: Counter increment uses `ConditionExpression: active < limit` to enforce the global limit atomically.

### Reservoir Connection Wrapper

The `reservoirConn` wraps a physical connection and returns it to the reservoir on `Close()`:

```go
func (c *reservoirConn) Close() error {
    if c.closed.Swap(true) {
        return nil  // Already closed
    }
    if c.bad.Load() {
        // Mark as expired to force discard
        c.pc.Lifetime = 0
    }
    c.r.Return(c.pc, time.Now().UTC())
    return nil
}
```

**Design Decisions:**

1. **Bad Connection Tracking**: If any operation returns `driver.ErrBadConn`, the `bad` flag is set. On `Close()`, bad connections are discarded rather than returned to reservoir.

2. **Interface Forwarding**: The wrapper forwards all optional driver interfaces (`ConnBeginTx`, `ExecerContext`, `QueryerContext`, `Pinger`, `SessionResetter`, `Validator`) to the underlying connection.

### Initial Fill Synchronization

When reservoir mode is enabled, the plugin waits for the reservoir to reach the low watermark before returning the connection:

```go
// In plugin.go, after registering reservoir driver:
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
for res.Len() < cfg.LowWatermark {
    select {
    case <-ctx.Done():
        logger.Warn("Reservoir initial fill timeout")
        break
    case <-time.After(100 * time.Millisecond):
    }
}
```

### Startup Logs

On successful reservoir startup, you'll see:

```
Reservoir refiller started  target_ready=50 low_watermark=50 base_lifetime=11m0s steady_state_interval=13.2s warmup_interval=10ms
Reservoir driver Open() - connection checked out  open_count=1 reservoir_ready=49
```

### When to Use Reservoir Mode

**Use reservoir mode when:**
- Running high-throughput workloads (>100 WPS)
- Multiple services share a DSQL cluster
- Connection expiry causes latency spikes
- Pool shrinkage is observed during burst expiry

**Stick with standard mode when:**
- Running low-throughput workloads
- Single service with dedicated DSQL cluster
- Connection pool is stable without intervention

## Pool Simulator

A discrete event simulator is available for validating reservoir behavior under various conditions without requiring a real DSQL cluster.

### Running the Simulator

```bash
# Run with default config (local development)
go run ./tools/poolsim

# Run with ECS 150 WPS scenario
go run ./tools/poolsim -config tools/poolsim/scenarios/ecs-150wps.yaml

# Output to CSV for analysis
go run ./tools/poolsim -config tools/poolsim/scenarios/local.yaml -out results.csv
```

### What It Models

- **Reservoir**: Channel-based buffer with guard window validation
- **Continuous Refiller**: Back-to-back connection creation with rate limiter as only throttle
- **Expiry Scanner**: Proactive eviction of expired/expiring connections
- **Rate Limiting**: DSQL's cluster-wide 100 connections/second limit
- **Workload Simulation**: Database queries consuming connections

### Assertions

The simulator validates behavior with configurable assertions:

| Assertion | Description |
|-----------|-------------|
| `assertMaxConnectsPerSec` | Global connection rate must not exceed 100/sec |
| `assertConvergeWithin` | All reservoirs must reach target within this duration |
| `assertStableFor` | After reaching target, reservoirs must stay stable |
| `assertZeroEmptyEvents` | No checkout attempts should find empty reservoir |

### Pre-Configured Scenarios

| Scenario | Services | Connections | Use Case |
|----------|----------|-------------|----------|
| `local.yaml` | 4 | 200 | Local development |
| `ecs-150wps.yaml` | 20 | 2,000 | ECS 150 WPS deployment |
| `ecs-400wps.yaml` | 44 | 22,000 | ECS 400 WPS deployment |

See `tools/poolsim/README.md` for full documentation.

## Testing

```bash
# Run DSQL-specific tests
go test ./common/persistence/sql/sqlplugin/dsql/...

# Run with verbose output
go test -v ./common/persistence/sql/sqlplugin/dsql/...

# Integration tests (requires DSQL cluster)
export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export REGION="us-east-1"
go test ./common/persistence/sql/sqlplugin/dsql/... -tags integration
```
