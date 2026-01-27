package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

// ReservoirConfig defines reservoir behavior parameters.
type ReservoirConfig struct {
	TargetReady  int
	LowWatermark int
	BaseLifetime time.Duration
	Jitter       time.Duration
	GuardWindow  time.Duration
}

// LeaseManager provides global connection-count lease acquisition/release.
type LeaseManager interface {
	Acquire(ctx context.Context) (string, error)
	Release(ctx context.Context, leaseID string) error
}

// reservoirDriver implements driver.Driver and sources connections from an in-process reservoir.
// Open() is non-blocking for the fast path, but may briefly wait when reservoir is empty.
type reservoirDriver struct {
	res              *Reservoir
	openCount        atomic.Int64
	logFunc          LogFunc
	metrics          ReservoirMetrics
	emptyWaitTimeout time.Duration
}

const reservoirDriverNamePrefix = "pgx-dsql-reservoir-"
const defaultEmptyWaitTimeout = 100 * time.Millisecond

var (
	reservoirDriverMu      sync.Mutex
	reservoirDriverCounter int
)

// RegisterReservoirDriverWithLogger registers a new reservoir-backed driver, starts the refiller loop,
// and returns the unique driver name to pass to sql.Open().
//
// IMPORTANT: Open() never blocks on any global limiter. All potentially blocking work is performed by
// the background refiller.
func RegisterReservoirDriverWithLogger(
	username string,
	baseDSN string,
	tokenProvider TokenProvider,
	rateLimiter RateLimiter,
	leaseManager LeaseManager,
	cfg ReservoirConfig,
	logFunc LogFunc,
	metrics ReservoirMetrics,
) (string, *Reservoir, error) {
	if tokenProvider == nil {
		return "", nil, fmt.Errorf("tokenProvider cannot be nil")
	}
	if username == "" {
		username = "admin"
	}
	if cfg.TargetReady <= 0 {
		return "", nil, fmt.Errorf("reservoir targetReady must be > 0")
	}
	if cfg.LowWatermark <= 0 {
		cfg.LowWatermark = cfg.TargetReady
	}
	if cfg.TargetReady < cfg.LowWatermark {
		cfg.TargetReady = cfg.LowWatermark
	}
	if cfg.BaseLifetime <= 0 {
		cfg.BaseLifetime = 11 * time.Minute
	}
	if cfg.GuardWindow < 0 {
		cfg.GuardWindow = 0
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}
	if metrics == nil {
		metrics = &noOpReservoirMetrics{}
	}

	reservoirDriverMu.Lock()
	reservoirDriverCounter++
	driverName := fmt.Sprintf("%s%d", reservoirDriverNamePrefix, reservoirDriverCounter)
	reservoirDriverMu.Unlock()

	res := NewReservoir(cfg.TargetReady, cfg.GuardWindow, leaseManager, logFunc, metrics)

	// Record target size metric
	metrics.RecordReservoirTarget(cfg.TargetReady)

	// Start refiller.
	refiller := &reservoirRefiller{
		username:      username,
		baseDSN:       baseDSN,
		res:           res,
		cfg:           cfg,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		leaseManager:  leaseManager,
		underlying:    stdlib.GetDefaultDriver(),
		logFunc:       logFunc,
		metrics:       metrics,
	}
	refiller.Start()

	wrapper := &reservoirDriver{res: res, logFunc: logFunc, metrics: metrics, emptyWaitTimeout: defaultEmptyWaitTimeout}
	sql.Register(driverName, wrapper)
	return driverName, res, nil
}

// Open implements driver.Driver.
// It first tries a non-blocking checkout. If the reservoir is empty, it waits briefly
// (up to emptyWaitTimeout) for the refiller to catch up before returning ErrBadConn.
func (d *reservoirDriver) Open(_ string) (driver.Conn, error) {
	start := time.Now()
	openNum := d.openCount.Add(1)
	now := start.UTC()

	// Try non-blocking first
	pc, ok := d.res.TryCheckout(now)
	if ok {
		d.metrics.RecordCheckoutLatency(time.Since(start))
		if d.logFunc != nil {
			d.logFunc("Reservoir driver Open() - connection checked out", "open_count", openNum, "reservoir_ready", d.res.Len())
		}
		return newReservoirConn(d.res, pc), nil
	}

	// Brief wait for refiller to catch up
	pc, ok = d.res.WaitCheckout(d.emptyWaitTimeout)
	if ok {
		d.metrics.RecordCheckoutLatency(time.Since(start))
		if d.logFunc != nil {
			d.logFunc("Reservoir driver Open() - connection checked out after wait", "open_count", openNum, "reservoir_ready", d.res.Len())
		}
		return newReservoirConn(d.res, pc), nil
	}

	// Record latency even on failure (will be the full wait timeout)
	d.metrics.RecordCheckoutLatency(time.Since(start))
	if d.logFunc != nil {
		d.logFunc("Reservoir driver Open() - no ready connections after wait", "open_count", openNum, "wait_timeout_ms", d.emptyWaitTimeout.Milliseconds())
	}
	return nil, driver.ErrBadConn
}

// injectToken creates a new DSN with the provided token as the password.
func injectToken(dsn, username, token string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	if parsed.User != nil && parsed.User.Username() != "" {
		username = parsed.User.Username()
	}
	parsed.User = url.UserPassword(username, token)
	return parsed.String(), nil
}
