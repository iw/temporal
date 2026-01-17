# DSQL Implementation Details

This document covers the technical implementation of Aurora DSQL support in Temporal.

## Code Structure

```
common/persistence/sql/sqlplugin/dsql/
├── plugin.go                 # Plugin registration and DB creation
├── db.go                     # Database handle and operations
├── metrics.go                # DSQL-specific metrics
├── retry.go                  # OCC retry logic
├── tx_retry_policy.go        # Transaction retry policy
├── errors.go                 # Error classification
├── token_cache.go            # IAM token caching
├── connection_rate_limiter.go # Connection rate limiting
├── driver/
│   └── token_refreshing.go   # Token-refreshing database driver
└── session/
    └── session.go            # Connection session management

common/persistence/sql/sqlplugin/
├── idgenerator.go            # Snowflake ID generator
└── idgenerator_test.go       # ID generator tests

schema/dsql/v12/temporal/
├── schema.sql                # DSQL-compatible schema
└── versioned/                # Schema migrations
```

## Plugin Registration

The DSQL plugin registers as `dsql` and creates connections with IAM authentication:

```go
func init() {
    persistencesql.RegisterPlugin(PluginName, &plugin{
        driver:         &driver.PGXDriver{},
        queryConverter: &queryConverter{},
        retryConfig:    DefaultRetryConfig(),
    })
}
```

## IAM Authentication

### Token-Refreshing Driver

The plugin uses a custom database driver that injects fresh IAM tokens for each new connection:

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

### Token Cache

Tokens are cached to avoid excessive IAM calls:

```go
type TokenCache struct {
    credentials aws.CredentialsProvider
    cache       sync.Map  // endpoint -> cachedToken
    logger      log.Logger
}

type cachedToken struct {
    token     string
    expiresAt time.Time
}
```

Default token duration is 14 minutes (DSQL tokens valid for 15 minutes).

## ID Generation

### Snowflake Algorithm

DSQL doesn't support `BIGSERIAL`, so we use Snowflake-style distributed IDs:

```go
type SnowflakeIDGenerator struct {
    nodeID     int64      // 10 bits: 0-1023
    sequence   int64      // 12 bits: 0-4095
    lastTime   int64      // 41 bits: milliseconds since epoch
    timeSource clock.TimeSource
    mu         sync.Mutex
}

func (g *SnowflakeIDGenerator) NextID() (int64, error) {
    g.mu.Lock()
    defer g.mu.Unlock()
    
    now := g.timeSource.Now().UnixMilli() - epoch
    
    if now == g.lastTime {
        g.sequence = (g.sequence + 1) & sequenceMask
        if g.sequence == 0 {
            // Wait for next millisecond
            for now <= g.lastTime {
                now = g.timeSource.Now().UnixMilli() - epoch
            }
        }
    } else {
        g.sequence = 0
    }
    
    g.lastTime = now
    return (now << timestampShift) | (g.nodeID << nodeIDShift) | g.sequence, nil
}
```

### Node ID Assignment

Node IDs are derived from hostname for distributed uniqueness:

```go
func GetNodeIDFromHostname(hostname string) int64 {
    h := fnv.New32a()
    h.Write([]byte(hostname))
    return int64(h.Sum32() & 0x3FF) // 10 bits
}
```

## Retry Logic

### OCC Conflict Handling

DSQL uses optimistic concurrency control. Serialization conflicts (SQLSTATE 40001) are retried:

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
            
            if isRetryable(err) && attempt < maxAttempts {
                r.metrics.IncTxConflict(op)
                r.metrics.IncTxRetry(op, attempt)
                
                delay := r.calculateBackoff(attempt - 1)
                time.Sleep(delay)
                continue
            }
            return nil, err
        }
        
        if err := tx.Commit(); err != nil {
            if isRetryable(err) && attempt < maxAttempts {
                delay := r.calculateBackoff(attempt - 1)
                time.Sleep(delay)
                continue
            }
            return nil, err
        }
        
        return result, nil
    }
    
    return nil, fmt.Errorf("max retries exceeded")
}
```

### Error Classification

```go
type ErrorType int

const (
    ErrorTypeUnknown ErrorType = iota
    ErrorTypeRetryable          // SQLSTATE 40001 - serialization failure
    ErrorTypeConditionFailed    // CAS condition not met
    ErrorTypePermanent          // Non-retryable errors
    ErrorTypeUnsupportedFeature // SQLSTATE 0A000
    ErrorTypeConnectionLimit    // Connection limit exceeded
    ErrorTypeTransactionTimeout // Transaction timeout
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
    return ErrorTypePermanent
}
```

## Connection Rate Limiting

DSQL has cluster-wide connection limits. The plugin implements per-instance rate limiting:

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

func (r *ConnectionRateLimiter) Wait(ctx context.Context) error {
    return r.limiter.Wait(ctx)
}
```

### Staggered Startup

To prevent thundering herd on service restart:

```go
func StaggeredStartupDelay() time.Duration {
    if !getEnvBool("DSQL_STAGGERED_STARTUP", true) {
        return 0
    }
    maxDelay := getEnvDuration("DSQL_STAGGERED_STARTUP_MAX_DELAY", 5*time.Second)
    return time.Duration(rand.Int63n(int64(maxDelay)))
}
```

## Schema Adaptations

### Constraint Handling

CHECK constraints are validated in application code:

```go
// nexus_endpoints_partition_status: only id=0 allowed
func (pdb *db) InsertIntoNexusEndpointsPartitionStatus(
    ctx context.Context, row *sqlplugin.NexusEndpointsPartitionStatusRow) error {
    
    if row.ID != 0 {
        return &ConditionFailedError{
            Msg: "nexus_endpoints_partition_status: only id=0 allowed",
        }
    }
    // ... insert logic
}
```

### Default Values

Complex defaults are set in application code:

```go
func (pdb *db) UpsertClusterMembership(
    ctx context.Context, row *sqlplugin.ClusterMembershipRow) error {
    
    if row.SessionStart.IsZero() {
        row.SessionStart = time.Unix(1, 0) // 1970-01-01 00:00:01
    }
    // ... upsert logic
}
```

## Pool Metrics Collection

A background goroutine samples connection pool statistics:

```go
func (m *dsqlMetricsImpl) StartPoolCollector(db *sql.DB, interval time.Duration) {
    go func() {
        ticker := time.NewTicker(interval)
        defer ticker.Stop()
        
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

### Unit Tests

```bash
# Run DSQL-specific tests
go test ./common/persistence/sql/sqlplugin/dsql/...

# Run ID generator tests
go test ./common/persistence/sql/sqlplugin/ -run TestSnowflake
```

### Integration Tests

Integration tests require a DSQL cluster:

```bash
export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export REGION="us-east-1"
go test ./common/persistence/sql/sqlplugin/dsql/... -tags integration
```
