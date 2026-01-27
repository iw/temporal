# AGENTS

## Mission
- Enable Temporal to run with Amazon Aurora DSQL as the **persistence** database while keeping the existing visibility store unchanged.
- Maintain Temporal feature parity, operational reliability, and performance envelopes established for the current persistence backends.
- Deliver incremental milestones that unblock experimentation without destabilizing the primary Temporal development branch.

## Scope & Non‑Goals
- **In scope:** schema compatibility, persistence layer wiring, config surfaces, tooling/tests for Aurora DSQL, migration/rollback paths, observability.
- **Not in scope:** changes to visibility DB, unrelated feature work, adopting new third‑party libraries unless usage already exists in `go.mod`.
- Avoid speculative refactors; tie every change to Aurora DSQL enablement or required hygiene uncovered along the way.

## Architecture Touchpoints
1. **`/common/persistence`**: storage interfaces, factory wiring, schema management utilities.
2. **`/service/*`**: ensure persistence clients are injectable/configurable per service without affecting visibility paths.
3. **`/schema`**: new/updated DSQL schema artifacts, migrations, and validation tooling.
4. **Config**: extend dynamic/static config knobs (under `/config` and `/common/dynamicconfig`) so operators can opt into DSQL.
5. **Tooling**: `make` targets, proto/codegen hooks, and deployment templates that reference persistence backends.

## Working Agreements
- Mirror existing code style, naming, and error handling patterns. Inspect neighboring files before editing.
- Verify any dependency is already used in the repo before importing it.
- Prefer incremental PRs grouped by subsystem (e.g., schema prep, persistence adapter, service wiring, tests).
- Keep comments minimal and focused on *why* design trade‑offs were made.
- Do not revert or overwrite local developer changes unless explicitly asked.

## Development Flow
1. **Understand**: read surrounding code/tests/config before coding; capture findings in issue notes or design docs.
2. **Plan**: break tasks into verifiable steps with explicit test strategies; call out performance, scalability, complexity, and security trade‑offs and failure modes (crash recovery, 10× load, etc.).
3. **Implement**: follow repo conventions, ensure error paths are handled/logged appropriately (`logger.Fatal` vs `logger.DPanic`).
4. **Regenerate**: rerun codegen whenever touching `.proto`, `//go:generate`, or schema assets.
5. **Verify**: run `make lint-code` plus targeted unit tests (`make unit-test -tags test_dep` as needed). Document skipped tests with rationale.

## Testing Expectations
- Cover success and failure cases for persistence adapters, config guards, and migrations.
- Include regression tests for mixed persistence/visibility backends.
- Leverage deterministic fixtures; avoid flaky integration tests unless tagged with `integration`.
- Prefer `require` over `assert`, avoid testify suites in unit tests, use `require.Eventually` instead of `time.Sleep`.

## Operational Considerations
- Document rollout/rollback instructions for operators (config flags, schema migrations, fallback options).
- Ensure metrics/alerts exist for Aurora DSQL health and latency; extend `/common/metrics` definitions when necessary.
- Capture open questions (consistency model, transaction semantics, failure domains) in tracking issues or this document.

---

## Implementation Status

### ✅ Completed

#### Schema Compatibility
- DSQL-compatible schema (`schema/dsql/v12/temporal/schema.sql`)
- BYTEA → UUID conversion for composite primary keys
- BIGSERIAL → BIGINT with Snowflake ID generation
- CHECK constraints removed (application-level validation)
- INDEX ASYNC for non-blocking index creation

#### Persistence Layer
- First-class DSQL plugin (`common/persistence/sql/sqlplugin/dsql/`)
- Optimistic Concurrency Control with retry logic
- Compare-And-Swap (CAS) updates for shard management
- FOR UPDATE on single table only (DSQL limitation)
- FOR SHARE delegated to FOR UPDATE

#### IAM Authentication
- Token-refreshing driver with automatic credential refresh
- Token cache with expiry management
- Connection rate limiting (local and distributed)

#### Connection Management - Reservoir Mode (Recommended)
- **Connection Reservoir** (`driver/reservoir.go`) - Channel-based buffer of pre-created connections
- **Continuous Refiller** (`driver/reservoir_refiller.go`) - Background goroutine that fills reservoir
- **Proactive Expiry Scanner** - Evicts connections before they expire
- **Guard Window** - Discards connections too close to expiry
- **Distributed Connection Leasing** - DynamoDB-backed global connection count limiting

#### Connection Management - Legacy Mode
- Pool Warmup - Sequential connection creation at startup
- Pool Keeper - Background maintenance for connection replacement

#### Observability
- DSQL-specific metrics (conflicts, retries, latency)
- Reservoir metrics (checkouts, empty events, discards, refills)
- Connection pool metrics (open, idle, in-use)

#### Documentation
- `docs/dsql/overview.md` - High-level overview
- `docs/dsql/implementation.md` - Technical implementation details
- `docs/dsql/reservoir-design.md` - Reservoir architecture and configuration
- `docs/dsql/metrics.md` - Metrics reference
- `docs/dsql/migration-guide.md` - Migration instructions

#### Tooling
- `tools/poolsim/` - Discrete event simulator for reservoir behavior validation
- `tools/dsql/` - DSQL schema management tool

### Known Constraints

1. **BIGSERIAL Not Supported**: Application generates IDs via Snowflake algorithm
2. **CHECK Constraints Not Supported**: Application-level validation
3. **FOR SHARE Not Supported**: Delegated to FOR UPDATE
4. **FOR UPDATE on JOINs Not Supported**: Split into separate queries
5. **Connection Rate Limit**: 100 connections/second cluster-wide
6. **Connection Limit**: 10,000 connections per cluster (default, can be raised)

### Recommended Configuration

**Reservoir Mode** (recommended for production):
```bash
export DSQL_RESERVOIR_ENABLED=true
export DSQL_RESERVOIR_TARGET_READY=50
export DSQL_RESERVOIR_BASE_LIFETIME=11m
export DSQL_RESERVOIR_LIFETIME_JITTER=2m
export DSQL_RESERVOIR_GUARD_WINDOW=45s
```

**Distributed Connection Leasing** (for multi-service deployments):
```bash
export DSQL_DISTRIBUTED_CONN_LEASE_ENABLED=true
export DSQL_DISTRIBUTED_CONN_LEASE_TABLE=temporal-dsql-conn-lease
export DSQL_DISTRIBUTED_CONN_LIMIT=10000
```

### Validation Results

- **400 WPS benchmark**: Validated with 100% workflow completion
- **Checkout latency**: Sub-millisecond (p99 < 1ms)
- **Zero empty events**: Reservoir always has connections available
- **Rate limit compliance**: Never exceeds 100 connections/second

---

## Documentation

| Document | Description |
|----------|-------------|
| `docs/dsql/overview.md` | High-level overview of DSQL support |
| `docs/dsql/implementation.md` | Technical implementation details |
| `docs/dsql/reservoir-design.md` | Reservoir architecture and configuration |
| `docs/dsql/metrics.md` | Metrics reference |
| `docs/dsql/migration-guide.md` | Migration from PostgreSQL to DSQL |
| `tools/poolsim/README.md` | Pool simulator documentation |
