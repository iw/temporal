# Aurora DSQL Migration Guide

## Overview
This guide covers migrating Temporal's persistence layer from PostgreSQL to Amazon Aurora DSQL. Aurora DSQL is a PostgreSQL-compatible serverless database that requires specific schema and application modifications for full compatibility.

## Prerequisites
- Aurora DSQL cluster provisioned and accessible
- Temporal version 1.18+ (schema compatibility baseline)
- PostgreSQL client tools for initial validation

## Schema Modifications Required

### 1. Auto-Increment Columns → Application ID Generation
**Issue**: DSQL doesn't support `BIGSERIAL` auto-increment.

**Migration**:
```sql
-- Before (PostgreSQL)
CREATE TABLE buffered_events (
    id BIGSERIAL NOT NULL,
    -- other columns
);

-- After (DSQL)
CREATE TABLE buffered_events (
    id BIGINT NOT NULL,
    -- other columns
);
CREATE UNIQUE INDEX idx_buffered_events_id ON buffered_events(id);
```

**Application Changes**:
- Implemented Snowflake-style ID generator in `common/persistence/sql/sqlplugin/idgenerator.go`
- Thread-safe distributed ID generation (4096 IDs/ms per node)
- Automatic node ID assignment based on hostname

### 2. CHECK Constraints → Application Validation
**Issue**: DSQL doesn't support CHECK constraints.

**Migration**:
```sql
-- Before (PostgreSQL)
CREATE TABLE nexus_endpoints_partition_status (
    id INTEGER NOT NULL,
    CONSTRAINT only_one_row CHECK (id = 0)
);

-- After (DSQL)
CREATE TABLE nexus_endpoints_partition_status (
    id INTEGER NOT NULL
);
```

**Application Changes**:
- Added validation in persistence layer operations
- Proper error handling for constraint violations

### 3. Complex DEFAULT Values → Application Defaults
**Issue**: DSQL has limited support for complex DEFAULT expressions.

**Migration**:
```sql
-- Before (PostgreSQL)
CREATE TABLE cluster_membership (
    session_start TIMESTAMP DEFAULT '1970-01-01 00:00:01+00:00'
);

-- After (DSQL)
CREATE TABLE cluster_membership (
    session_start TIMESTAMP
);
```

**Application Changes**:
- Set default values in Go code before INSERT operations
- Consistent default handling across all operations

### 4. Inline UNIQUE → Separate Indexes
**Enhancement**: Better DSQL compatibility with explicit indexes.

**Migration**:
```sql
-- Before (PostgreSQL)
CREATE TABLE namespaces (
    name VARCHAR(255) UNIQUE NOT NULL
);

-- After (DSQL)
CREATE TABLE namespaces (
    name VARCHAR(255) NOT NULL
);
CREATE UNIQUE INDEX idx_namespaces_name ON namespaces(name);
```

### 5. Performance Optimizations
**Enhancement**: Leverage DSQL-specific features.

```sql
-- Use async index creation for better performance
CREATE INDEX ASYNC cm_idx_rolehost ON cluster_membership (role, host_id);
```

## Configuration Changes

### 1. Connection Configuration
Update your Temporal configuration to use DSQL:

```yaml
# config/development.yaml
persistence:
  defaultStore: dsql-default
  datastores:
    dsql-default:
      sql:
        pluginName: "dsql"
        databaseName: "temporal"
        connectAddr: "your-dsql-cluster.dsql.us-east-1.on.aws:5432"
        connectProtocol: "tcp"
        user: "temporal_user"
        password: "temporal_password"
        connectAttributes:
          sslmode: "require"
        maxConns: 20
        maxIdleConns: 20
        maxConnLifetime: "1h"
```

### 2. Dynamic Configuration
Optimize for DSQL's characteristics:

```yaml
# config/dynamicconfig/development.yaml
persistence.enableDSQLMode:
  - value: true
    constraints: {}

# Higher retry counts for optimistic concurrency control
history.persistenceMaxQPS:
  - value: 3000
    constraints: {}

# Connection pooling for serverless architecture
persistence.maxConns:
  - value: 20
    constraints: {}
```

## Migration Steps

### Step 1: Schema Migration
1. **Create DSQL database**:
   ```bash
   temporal-sql-tool --plugin dsql --ep your-dsql-cluster.dsql.us-east-1.on.aws:5432 create-database
   ```

2. **Apply DSQL schema**:
   ```bash
   temporal-sql-tool --plugin dsql --ep your-dsql-cluster.dsql.us-east-1.on.aws:5432 setup-schema -v 0.0
   temporal-sql-tool --plugin dsql --ep your-dsql-cluster.dsql.us-east-1.on.aws:5432 update-schema -d schema/dsql/v12/temporal/versioned
   ```

### Step 2: Data Migration (if applicable)
For existing PostgreSQL data:

1. **Export data** from PostgreSQL
2. **Transform ID columns** (remove auto-increment, ensure unique values)
3. **Import to DSQL** with proper ID generation

### Step 3: Application Deployment
1. **Update configuration** files with DSQL connection details
2. **Deploy Temporal services** with DSQL plugin
3. **Verify connectivity** and basic operations

### Step 4: Validation
1. **Test ID generation** under load
2. **Verify constraint validation** in application
3. **Monitor for serialization conflicts**
4. **Validate performance** meets requirements

## Error Handling

### Serialization Conflicts
DSQL uses optimistic concurrency control. Handle conflicts:

```go
// Example retry logic for serialization conflicts
if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "40001" {
    // Retry the operation
    return &persistence.ConditionFailedError{
        Msg: fmt.Sprintf("DSQL serialization conflict: %s", pqErr.Message),
    }
}
```

### Connection Errors
Monitor and handle DSQL-specific connection patterns:

```go
// DSQL connection error detection
dsqlErrors := []string{
    "dsql cluster unavailable",
    "dsql endpoint not found",
    "connection to dsql failed",
}
```

## Performance Considerations

### 1. Connection Pooling
- DSQL is serverless - optimize connection patterns
- Use appropriate pool sizes (recommended: 20 max connections)
- Monitor connection metrics and latency

### 2. Transaction Patterns
- Implement retry logic for serialization conflicts
- Keep transactions small and focused
- Monitor transaction success rates

### 3. Index Strategy
- Use `CREATE INDEX ASYNC` for large tables
- Monitor index creation progress
- Plan index maintenance windows

## Rollback Procedures

### Emergency Rollback to PostgreSQL
1. **Switch configuration** back to PostgreSQL
2. **Restart Temporal services**
3. **Verify data consistency**
4. **Monitor for issues**

### Gradual Migration
1. **Use feature flags** to control DSQL usage
2. **Implement dual-write patterns** during transition
3. **Validate data consistency** between systems
4. **Gradually increase DSQL traffic**

## Monitoring & Alerting

### Key Metrics to Monitor
- **Connection pool utilization**
- **Serialization conflict rate**
- **Query latency (p50, p95, p99)**
- **Error rates by type**
- **ID generation performance**

### Recommended Alerts
- High serialization conflict rate (>5%)
- Connection pool exhaustion
- Query latency above baseline
- DSQL cluster unavailability

## Troubleshooting

### Common Issues

1. **High Serialization Conflicts**
   - Reduce transaction size
   - Implement exponential backoff
   - Review concurrent access patterns

2. **Connection Pool Exhaustion**
   - Increase pool size
   - Reduce connection lifetime
   - Monitor connection leaks

3. **ID Generation Collisions**
   - Verify node ID uniqueness
   - Check system clock synchronization
   - Monitor ID generation rate

## Support & Resources

- **Implementation**: See `common/persistence/sql/sqlplugin/dsql/` for DSQL plugin
- **Configuration**: Examples in `config/development-dsql.yaml`
- **Testing**: Unit tests in `common/persistence/sql/sqlplugin/idgenerator_test.go`
- **Documentation**: `docs/dsql-schema-compatibility.md` for detailed compatibility matrix

## Next Steps

After successful migration:
1. **Performance tuning** based on actual workload
2. **Multi-region deployment** planning
3. **Disaster recovery** procedures
4. **Capacity planning** for growth