# Aurora DSQL Integration for Temporal

Aurora DSQL is Amazon's serverless, PostgreSQL-compatible distributed SQL database. This integration enables Temporal to use DSQL as its persistence layer while maintaining full feature parity with existing SQL backends.

## Schema Version

**Current DSQL Schema Version: 1.1**

The DSQL schema includes support for:
- Workflow executions (`current_executions`, `executions`)
- CHASM executions (`current_chasm_executions`) - standalone activities, schedulers
- CHASM component trees (`chasm_node_maps`)
- Task queues, timers, replication, and visibility tasks

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Temporal Services                        │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ │
│  │  Frontend   │ │   History   │ │  Matching   │ │   Worker    │ │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ │
└─────────┼───────────────┼───────────────┼───────────────┼───────┘
          │               │               │               │
          └───────────────┴───────┬───────┴───────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────┐
│                      DSQL Persistence Layer                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  DSQL Plugin    │  │  ID Generator   │  │  Retry Manager  │  │
│  │                 │  │                 │  │                 │  │
│  │ • IAM Auth      │  │ • Snowflake     │  │ • OCC Handling  │  │
│  │ • Token Refresh │  │   Algorithm     │  │ • Backoff       │  │
│  │ • Rate Limiting │  │ • Distributed   │  │ • Metrics       │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────┐
│                      Aurora DSQL Cluster                        │
│  • PostgreSQL Wire Protocol    • Serverless Scaling             │
│  • Optimistic Concurrency      • Multi-AZ Availability          │
│  • IAM Authentication          • Automatic Backups              │
└─────────────────────────────────────────────────────────────────┘
```

## Key Features

### IAM Authentication with Token Refresh
- Automatic IAM token generation using AWS credentials
- Token caching with configurable duration (default: 14 minutes)
- Token-refreshing database driver that injects fresh tokens for each new connection
- No stored passwords - credentials derived from IAM roles

### Optimistic Concurrency Control (OCC)
- DSQL uses OCC instead of pessimistic locking
- Automatic retry logic for serialization conflicts (SQLSTATE 40001)
- Exponential backoff with jitter
- Configurable retry limits and delays

### Distributed ID Generation
- Snowflake-style algorithm replacing PostgreSQL's BIGSERIAL
- 41-bit timestamp, 10-bit node ID, 12-bit sequence
- 4096 IDs per millisecond per node
- Zero collision rate under concurrent load

### Connection Rate Limiting

DSQL has a cluster-wide limit of 100 new connections per second. The plugin provides two rate limiting modes:

#### Local Rate Limiting (Default)
- Per-instance rate limiting using token bucket algorithm
- Requires manual partitioning of the 100/sec budget across instances
- Configurable via environment variables

#### Distributed Rate Limiting (Recommended for Production)
- DynamoDB-backed coordination across all service instances
- Automatically enforces the cluster-wide 100/sec limit
- No manual partitioning required - scales automatically with service count
- Per-second atomic counters with conditional updates
- Graceful fallback to local limiting if DynamoDB unavailable

**Important**: Rate limiting only applies to NEW connection establishment (TCP/TLS handshake + IAM authentication), not to queries. Once a connection is in the pool, queries flow through without rate limiting.

### Connection Reservoir (Advanced)

For high-throughput workloads, the plugin provides an optional Connection Reservoir mode that maintains a buffer of pre-created connections:

- Decouples connection acquisition from rate-limited creation
- Background refiller continuously maintains the reservoir
- Non-blocking `driver.Open()` for instant connection checkout
- Automatic connection lifecycle management with jittered expiry
- Optional DynamoDB-backed global connection count limiting

See [Reservoir Design](reservoir-design.md) for detailed documentation.

## Configuration

### Basic Configuration

```yaml
persistence:
  defaultStore: dsql-default
  datastores:
    dsql-default:
      sql:
        pluginName: "dsql"
        databaseName: "temporal"
        connectAddr: "your-cluster.dsql.us-east-1.on.aws:5432"
        maxConns: 20
        maxIdleConns: 10
```

### Environment Variables

#### Core Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLUSTER_ENDPOINT` | - | DSQL cluster endpoint |
| `REGION` or `AWS_REGION` | - | AWS region for IAM auth |
| `DSQL_TOKEN_DURATION` | `14m` | IAM token validity period |

#### Local Rate Limiting

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_CONNECTION_RATE_LIMIT` | `10` | Connections per second per instance |
| `DSQL_CONNECTION_BURST_LIMIT` | `100` | Connection burst capacity |
| `DSQL_STAGGERED_STARTUP` | `true` | Enable random startup delay |
| `DSQL_STAGGERED_STARTUP_MAX_DELAY` | `5s` | Maximum startup delay |

#### Distributed Rate Limiting

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED` | `false` | Enable DynamoDB-backed distributed rate limiting |
| `DSQL_DISTRIBUTED_RATE_LIMITER_TABLE` | - | DynamoDB table name (required if enabled) |
| `DSQL_DISTRIBUTED_RATE_LIMITER_LIMIT` | `100` | Cluster-wide connections per second |
| `DSQL_DISTRIBUTED_RATE_LIMITER_MAX_WAIT` | `30s` | Maximum wait time for connection permit |

#### Connection Reservoir (Advanced)

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_RESERVOIR_ENABLED` | `false` | Enable reservoir mode |
| `DSQL_RESERVOIR_TARGET_READY` | `maxOpen` | Target reservoir size |
| `DSQL_RESERVOIR_LOW_WATERMARK` | `maxOpen` | Aggressive refill threshold |
| `DSQL_RESERVOIR_BASE_LIFETIME` | `11m` | Base connection lifetime |
| `DSQL_RESERVOIR_LIFETIME_JITTER` | `2m` | Lifetime jitter range |
| `DSQL_RESERVOIR_GUARD_WINDOW` | `45s` | Discard if remaining lifetime within this |

#### Distributed Connection Leasing (with Reservoir)

| Variable | Default | Description |
|----------|---------|-------------|
| `DSQL_DISTRIBUTED_CONN_LEASE_ENABLED` | `false` | Enable global connection count limiting |
| `DSQL_DISTRIBUTED_CONN_LEASE_TABLE` | - | DynamoDB table name for leases |
| `DSQL_DISTRIBUTED_CONN_LIMIT` | `10000` | Global connection limit |

### Dynamic Configuration

```yaml
# Enable DSQL mode
persistence.enableDSQLMode:
  - value: true

# Persistence throughput
history.persistenceMaxQPS:
  - value: 9000

# Connection pool
persistence.maxConns:
  - value: 50

persistence.maxIdleConns:
  - value: 25
```

## DSQL Constraints

Aurora DSQL has specific limitations that the plugin handles:

| PostgreSQL Feature | DSQL Support | Solution |
|-------------------|--------------|----------|
| `BIGSERIAL` | ❌ | Snowflake ID generator |
| `CHECK` constraints | ❌ | Application-level validation |
| Complex `DEFAULT` | ⚠️ Limited | Application-level defaults |
| Foreign keys | ⚠️ Not enforced | JOINs work, integrity is application-managed |
| Pessimistic locking | ❌ | OCC with retry logic |

## Documentation

- **[Implementation Details](implementation.md)** - Technical implementation, code structure, and internals
- **[Metrics Reference](metrics.md)** - Available metrics for monitoring and alerting
- **[Migration Guide](migration-guide.md)** - Migrating from PostgreSQL to DSQL
- **[Reservoir Design](reservoir-design.md)** - Connection reservoir architecture and configuration

## Quick Start

1. **Create DSQL cluster** in AWS Console or via Terraform

2. **Configure IAM permissions** for Temporal services:
   
   The plugin currently uses admin authentication, which requires:
   - `dsql:DbConnectAdmin` - Admin access for schema operations and runtime
   
   Example IAM policy:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": "dsql:DbConnectAdmin",
       "Resource": "arn:aws:dsql:REGION:ACCOUNT:cluster/CLUSTER_ID"
     }]
   }
   ```
   
   See [Aurora DSQL IAM documentation](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/security-iam.html) for details on IAM roles and policies.

3. **(Optional) Create DynamoDB table for distributed rate limiting**:
   
   If using distributed rate limiting (recommended for production), create a DynamoDB table:
   
   ```bash
   aws dynamodb create-table \
     --table-name temporal-dsql-rate-limit \
     --attribute-definitions AttributeName=pk,AttributeType=S \
     --key-schema AttributeName=pk,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST \
     --region us-east-1
   
   # Enable TTL for automatic cleanup
   aws dynamodb update-time-to-live \
     --table-name temporal-dsql-rate-limit \
     --time-to-live-specification Enabled=true,AttributeName=ttl_epoch \
     --region us-east-1
   ```
   
   Add DynamoDB permissions to your IAM policy:
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "dynamodb:UpdateItem"
     ],
     "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/temporal-dsql-rate-limit"
   }
   ```

4. **Set environment variables**:
   ```bash
   export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
   export REGION="us-east-1"
   
   # For distributed rate limiting (optional but recommended)
   export DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED="true"
   export DSQL_DISTRIBUTED_RATE_LIMITER_TABLE="temporal-dsql-rate-limit"
   ```

5. **Initialize schema**:
   ```bash
   temporal-dsql-tool --endpoint $CLUSTER_ENDPOINT --region $REGION \
     setup-schema --schema-name "dsql/temporal" --version 1.1
   ```

6. **Start Temporal** with DSQL configuration
