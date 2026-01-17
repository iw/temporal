# Aurora DSQL Integration for Temporal

Aurora DSQL is Amazon's serverless, PostgreSQL-compatible distributed SQL database. This integration enables Temporal to use DSQL as its persistence layer while maintaining full feature parity with existing SQL backends.

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
- Per-instance rate limiting to respect DSQL cluster limits
- Configurable via environment variables
- Staggered startup to prevent thundering herd

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

| Variable | Default | Description |
|----------|---------|-------------|
| `CLUSTER_ENDPOINT` | - | DSQL cluster endpoint |
| `REGION` or `AWS_REGION` | - | AWS region for IAM auth |
| `DSQL_TOKEN_DURATION` | `14m` | IAM token validity period |
| `DSQL_CONNECTION_RATE_LIMIT` | `10` | Connections per second per instance |
| `DSQL_CONNECTION_BURST_LIMIT` | `100` | Connection burst capacity |
| `DSQL_STAGGERED_STARTUP` | `true` | Enable random startup delay |

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

- **[Implementation Details](dsql-implementation.md)** - Technical implementation, code structure, and internals
- **[Metrics Reference](dsql-metrics.md)** - Available metrics for monitoring and alerting
- **[Migration Guide](dsql-migration-guide.md)** - Migrating from PostgreSQL to DSQL

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

3. **Set environment variables**:
   ```bash
   export CLUSTER_ENDPOINT="your-cluster.dsql.us-east-1.on.aws"
   export REGION="us-east-1"
   ```

4. **Initialize schema**:
   ```bash
   temporal-dsql-tool --endpoint $CLUSTER_ENDPOINT --region $REGION \
     setup-schema --schema-name "dsql/v12/temporal" --version 1.12
   ```

5. **Start Temporal** with DSQL configuration
