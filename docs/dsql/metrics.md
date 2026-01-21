# DSQL Metrics Reference

The DSQL plugin emits metrics for monitoring database operations, connection pool health, and retry behavior.

## Transaction Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dsql_tx_latency` | Timer | `operation` | Total latency of transaction operations |
| `dsql_tx_error_class_total` | Counter | `operation`, `error_class` | Errors by classification |
| `dsql_tx_exhausted_total` | Counter | `operation` | Retry budget exhausted |
| `dsql_tx_conflict_total` | Counter | `operation` | Serialization conflicts (SQLSTATE 40001) |
| `dsql_tx_retry_total` | Counter | `operation`, `attempt` | Retry attempts |
| `dsql_tx_backoff` | Timer | `operation` | Backoff delay durations |

### Error Classes

The `error_class` label can have these values:

| Value | Description |
|-------|-------------|
| `retryable` | Serialization conflict, will be retried |
| `condition_failed` | CAS condition not met |
| `permanent` | Non-retryable error |
| `unsupported_feature` | DSQL unsupported feature |
| `connection_limit` | Connection limit exceeded |
| `transaction_timeout` | Transaction timed out |
| `unknown` | Unclassified error |

## Connection Pool Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dsql_pool_max_open` | Gauge | Maximum allowed open connections |
| `dsql_pool_open` | Gauge | Current number of open connections |
| `dsql_pool_in_use` | Gauge | Connections currently in use |
| `dsql_pool_idle` | Gauge | Idle connections in pool |
| `dsql_pool_wait_total` | Counter | Times a connection was waited for |
| `dsql_pool_wait_duration` | Timer | Total time spent waiting for connections |

### Connection Closure Metrics

These metrics track WHY connections are being closed, helping diagnose pool decay:

| Metric | Type | Description |
|--------|------|-------------|
| `dsql_db_closed_max_lifetime_total` | Gauge | Connections closed due to `MaxConnLifetime` (expected after 55 min) |
| `dsql_db_closed_max_idle_time_total` | Gauge | Connections closed due to `MaxConnIdleTime` (should be 0 if configured correctly) |
| `dsql_db_closed_max_idle_total` | Gauge | Connections closed because idle pool was full (shouldn't happen if `MaxIdleConns = MaxConns`) |

**Interpreting closure metrics:**

- **`dsql_db_closed_max_lifetime_total` increasing**: Normal - connections are being rotated after 55 minutes
- **`dsql_db_closed_max_idle_time_total` increasing**: Problem - `MaxConnIdleTime` should be 0 to prevent pool decay
- **`dsql_db_closed_max_idle_total` increasing**: Problem - `MaxIdleConns` should equal `MaxConns`
- **Pool shrinking but closure counters flat**: Server/network is closing connections (check DSQL logs, network issues)

Pool metrics are sampled every 15 seconds by a background collector.

## Derived Metrics

### Pool Saturation

```promql
# Pool saturation ratio (0-1)
dsql_pool_in_use / dsql_pool_max_open

# Alert when pool is >80% saturated
dsql_pool_in_use / dsql_pool_max_open > 0.8
```

### Connection Contention

```promql
# Connection wait rate (per second)
rate(dsql_pool_wait_total[5m])

# Average wait time per connection
rate(dsql_pool_wait_duration_sum[5m]) / rate(dsql_pool_wait_total[5m])
```

### OCC Conflict Rate

```promql
# Conflict rate per operation
rate(dsql_tx_conflict_total[5m])

# Conflict ratio (conflicts / total transactions)
rate(dsql_tx_conflict_total[5m]) / rate(dsql_tx_latency_count[5m])
```

### Retry Effectiveness

```promql
# Retry exhaustion rate (failures after max retries)
rate(dsql_tx_exhausted_total[5m])

# Average retries per conflict
rate(dsql_tx_retry_total[5m]) / rate(dsql_tx_conflict_total[5m])
```

## Alerting Recommendations

### Critical Alerts

```yaml
# High conflict rate - indicates contention issues
- alert: DSQLHighConflictRate
  expr: rate(dsql_tx_conflict_total[5m]) > 10
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "High DSQL serialization conflict rate"

# Retry exhaustion - operations failing after all retries
- alert: DSQLRetryExhaustion
  expr: rate(dsql_tx_exhausted_total[5m]) > 0
  for: 2m
  labels:
    severity: critical
  annotations:
    summary: "DSQL operations failing after retry exhaustion"

# Pool saturation
- alert: DSQLPoolSaturation
  expr: dsql_pool_in_use / dsql_pool_max_open > 0.9
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "DSQL connection pool >90% saturated"

# Connection wait time
- alert: DSQLConnectionWait
  expr: rate(dsql_pool_wait_duration_sum[5m]) / rate(dsql_pool_wait_total[5m]) > 1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Average connection wait time >1s"
```

### Informational Alerts

```yaml
# Transaction latency
- alert: DSQLHighLatency
  expr: histogram_quantile(0.99, rate(dsql_tx_latency_bucket[5m])) > 0.5
  for: 10m
  labels:
    severity: info
  annotations:
    summary: "DSQL p99 latency >500ms"
```

## Grafana Dashboard Queries

### Transaction Overview

```promql
# Transaction rate by operation
sum by (operation) (rate(dsql_tx_latency_count[5m]))

# P50/P95/P99 latency
histogram_quantile(0.50, rate(dsql_tx_latency_bucket[5m]))
histogram_quantile(0.95, rate(dsql_tx_latency_bucket[5m]))
histogram_quantile(0.99, rate(dsql_tx_latency_bucket[5m]))

# Error rate by class
sum by (error_class) (rate(dsql_tx_error_class_total[5m]))
```

### Connection Pool

```promql
# Pool utilization over time
dsql_pool_in_use
dsql_pool_idle
dsql_pool_max_open

# Connection churn
rate(dsql_pool_wait_total[5m])

# Connection closures by reason (helps diagnose pool decay)
dsql_db_closed_max_lifetime_total   # Expected: increases every 55 min
dsql_db_closed_max_idle_time_total  # Should be 0 if MaxConnIdleTime=0
dsql_db_closed_max_idle_total       # Should be 0 if MaxIdleConns=MaxConns
```

### OCC Performance

```promql
# Conflicts vs successful transactions
rate(dsql_tx_conflict_total[5m])
rate(dsql_tx_latency_count[5m]) - rate(dsql_tx_conflict_total[5m])

# Retry distribution
sum by (attempt) (rate(dsql_tx_retry_total[5m]))

# Backoff time spent
rate(dsql_tx_backoff_sum[5m])
```

## Legacy Metrics

These metrics are maintained for backward compatibility:

| Metric | Type | Description |
|--------|------|-------------|
| `dsql_tx_retries_total` | Counter | Retry attempts (legacy) |
| `dsql_tx_conflicts_total` | Counter | Conflicts (legacy) |
| `dsql_condition_failed_total` | Counter | CAS failures |
| `dsql_operation_duration` | Timer | Operation duration |
| `dsql_retry_attempts` | Histogram | Retry attempt distribution |
| `dsql_active_transactions` | Gauge | Active transaction count |
| `dsql_errors_total` | Counter | Errors by type |
| `dsql_unsupported_feature_total` | Counter | Unsupported feature usage |

## Metric Collection

Metrics are collected via Temporal's standard metrics system and exported to your configured metrics backend (Prometheus, StatsD, etc.).

### Prometheus Scrape Config

```yaml
scrape_configs:
  - job_name: 'temporal'
    static_configs:
      - targets: ['temporal-frontend:9090', 'temporal-history:9090', 
                  'temporal-matching:9090', 'temporal-worker:9090']
```

### Key Metrics to Monitor

1. **`dsql_tx_conflict_total`** - Primary indicator of OCC contention
2. **`dsql_pool_in_use / dsql_pool_max_open`** - Pool saturation
3. **`dsql_tx_exhausted_total`** - Failed operations after retries
4. **`dsql_tx_latency`** - Overall database performance
