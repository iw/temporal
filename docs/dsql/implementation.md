# DSQL Implementation Details

This document covers the technical implementation of Aurora DSQL support in Temporal, including schema changes, OCC handling, and the major contributions to achieve DSQL compatibility.

## Code Structure

```
common/persistence/sql/sqlplugin/dsql/
├── plugin.go                 # Plugin registration, IAM auth, token refresh
├── db.go                     # Database handle, transaction management
├── execution.go              # Workflow execution operations
├── shard.go                  # Shard management with CAS updates
├── cas_updates.go            # Compare-And-Swap update patterns
├── fenced_updates.go         # Fenced update helpers
├── retry.go                  # OCC retry logic with backoff
├── tx_retry_policy.go        # Transaction-level retry policy
├── errors.go                 # Error classification (retryable vs permanent)
├── metrics.go                # DSQL-specific metrics
├── token_cache.go            # IAM token caching
├── connection_rate_limiter.go # Connection rate limiting
├── uuid.go                   # UUID string conversion utilities
├── typeconv.go               # DateTime conversion
├── driver/
│   └── token_refreshing.go   # Token-refreshing database driver
└── session/
    └── session.go            # Connection session management

schema/dsql/v12/temporal/
└── schema.sql                # DSQL-compatible schema
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

DSQL has cluster-wide connection limits (100/sec, 1000 burst).

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
