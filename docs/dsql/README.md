# Aurora DSQL Support for Temporal

Aurora DSQL is Amazon's serverless, PostgreSQL-compatible distributed SQL database. This plugin enables Temporal to use DSQL as its persistence layer with full feature parity.

## Key Capabilities

- **IAM authentication** with automatic token refresh — no stored passwords
- **Optimistic concurrency control** with automatic retry and backoff
- **Connection Reservoir** for rate-limit-aware connection management
- **Distributed coordination** via DynamoDB for multi-service deployments
- **Snowflake ID generation** replacing PostgreSQL's BIGSERIAL

## Documentation

| Document | Description |
|----------|-------------|
| [Deployment Guide](deployment.md) | Production deployment: schema setup, configuration, and operational guidance |
| [Reservoir Design](reservoir-design.md) | Connection reservoir architecture, internals, and troubleshooting |
| [Implementation Details](implementation.md) | Code structure, schema changes, OCC handling, CAS updates |
| [Metrics Reference](metrics.md) | All emitted metrics with alerting recommendations |

## Quick Reference

**Schema version:** 1.1

**DSQL constraints handled by the plugin:**

| PostgreSQL Feature | DSQL Support | Plugin Solution |
|-------------------|--------------|-----------------|
| `BIGSERIAL` | Not supported | Snowflake ID generator |
| `CHECK` constraints | Not supported | Application-level validation |
| Pessimistic locking | Not supported | OCC with retry logic |
| `FOR UPDATE` on JOINs | Not supported | Split into separate queries |
| `FOR SHARE` | Not supported | Delegated to `FOR UPDATE` |

## Architecture

```
Temporal Services (Frontend, History, Matching, Worker)
                    │
                    ▼
         DSQL Persistence Layer
    ┌────────────┬────────────┬──────────────┐
    │ IAM Auth   │ Reservoir  │ OCC Retry    │
    │ + Token    │ + Refiller │ + Backoff    │
    │   Refresh  │ + Leasing  │ + Metrics    │
    └────────────┴────────────┴──────────────┘
                    │
                    ▼
           Aurora DSQL Cluster
```
