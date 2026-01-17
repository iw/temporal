# Aurora DSQL Migration Guide

This guide covers deploying Temporal with Aurora DSQL as the persistence layer.

## Prerequisites

- Aurora DSQL cluster with public endpoint enabled
- IAM permissions: `dsql:DbConnect`, `dsql:DbConnectAdmin`
- AWS credentials configured (IAM role, environment variables, or credentials file)
- `temporal-dsql-tool` binary for schema setup

## Schema Setup

### Using temporal-dsql-tool

```bash
# Set environment
export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export REGION="us-east-1"

# Setup schema with versioning (required for Temporal server)
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    setup-schema \
    --schema-name "dsql/v12/temporal" \
    --version 1.12

# Reset schema (drops and recreates)
./temporal-dsql-tool \
    --endpoint "$CLUSTER_ENDPOINT" \
    --region "$REGION" \
    setup-schema \
    --schema-name "dsql/v12/temporal" \
    --version 1.12 \
    --overwrite
```

The tool uses IAM authentication automatically and has the DSQL schema embedded.

## Configuration

### Static Configuration

```yaml
persistence:
  defaultStore: dsql-default
  datastores:
    dsql-default:
      sql:
        pluginName: "dsql"
        databaseName: "temporal"
        connectAddr: "${CLUSTER_ENDPOINT}:5432"
        maxConns: 50
        maxIdleConns: 25
        maxConnLifetime: "1h"
```

### Environment Variables

```bash
# Required
export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
export REGION="us-east-1"  # or AWS_REGION

# Optional - Token refresh
export DSQL_TOKEN_DURATION="14m"  # Default: 14 minutes

# Optional - Connection rate limiting
export DSQL_CONNECTION_RATE_LIMIT="10"   # Per-instance rate
export DSQL_CONNECTION_BURST_LIMIT="100" # Burst capacity
export DSQL_STAGGERED_STARTUP="true"     # Random startup delay
```

### Dynamic Configuration

```yaml
# Enable DSQL mode
persistence.enableDSQLMode:
  - value: true

# Throughput settings
history.persistenceMaxQPS:
  - value: 9000

matching.persistenceMaxQPS:
  - value: 9000

frontend.persistenceMaxQPS:
  - value: 9000

# Connection pool
persistence.maxConns:
  - value: 50

persistence.maxIdleConns:
  - value: 25
```

## Service-Specific Rate Limits

When running multiple Temporal service replicas, partition the DSQL connection rate limit:

| Service | Replicas | Rate/Instance | Total Rate |
|---------|----------|---------------|------------|
| History | 4 | 15/sec | 60/sec |
| Matching | 3 | 8/sec | 24/sec |
| Frontend | 2 | 5/sec | 10/sec |
| Worker | 2 | 3/sec | 6/sec |
| **Total** | **11** | - | **~100/sec** |

DSQL cluster limit is 100 connections/sec with 1,000 burst capacity.

## Verification

### Check Connectivity

```bash
# Using psql with IAM token
TOKEN=$(aws dsql generate-db-connect-admin-auth-token \
    --hostname "$CLUSTER_ENDPOINT" \
    --region "$REGION")

psql "host=$CLUSTER_ENDPOINT user=admin password=$TOKEN sslmode=require" \
    -c "SELECT version();"
```

### Verify Schema

```sql
-- Check schema version
SELECT * FROM schema_version;

-- List tables
SELECT tablename FROM pg_tables WHERE schemaname = 'public';
```

### Test Temporal

```bash
# Create namespace
temporal operator namespace create default

# Run a workflow
temporal workflow start --type MyWorkflow --task-queue my-queue
```

## Troubleshooting

### Connection Errors

**"REGION or AWS_REGION environment variable must be set"**
- Set `REGION` or `AWS_REGION` environment variable

**"failed to resolve AWS credentials"**
- Verify IAM role/credentials are configured
- Check `dsql:DbConnect` permission

**"connection rate limit exceeded"**
- Reduce `DSQL_CONNECTION_RATE_LIMIT`
- Enable `DSQL_STAGGERED_STARTUP`

### Serialization Conflicts

**High conflict rate (>5%)**
- Normal under high concurrency
- Retry logic handles automatically
- Monitor `dsql_tx_conflict_total` metric

**Retry exhaustion**
- Increase `MaxRetries` in retry config
- Check for hot spots in data access patterns

### Pool Exhaustion

**"connection pool exhausted"**
- Increase `maxConns` in configuration
- Check for connection leaks
- Monitor `dsql_pool_in_use` metric

## Rollback to PostgreSQL

1. Update configuration to use PostgreSQL plugin
2. Restart Temporal services
3. Verify connectivity and data access

```yaml
persistence:
  defaultStore: postgres-default
  datastores:
    postgres-default:
      sql:
        pluginName: "postgres12"
        connectAddr: "postgres-host:5432"
        # ... PostgreSQL settings
```

## Monitoring

Key metrics to monitor:

- `dsql_tx_conflict_total` - OCC conflicts
- `dsql_tx_exhausted_total` - Failed after retries
- `dsql_pool_in_use / dsql_pool_max_open` - Pool saturation
- `dsql_tx_latency` - Transaction latency

See [Metrics Reference](dsql-metrics.md) for complete list and alerting recommendations.
