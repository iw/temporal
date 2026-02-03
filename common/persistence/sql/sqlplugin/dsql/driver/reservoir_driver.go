package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

// ErrReservoirEmpty is returned when the reservoir has no connections available.
// This is a transient backpressure signal, NOT a corrupted connection error.
// IMPORTANT: This error intentionally does NOT wrap driver.ErrBadConn to prevent
// DatabaseHandle.ConvertError from triggering a full pool recreation.
// The persistence layer should treat this as a retryable transient error.
var ErrReservoirEmpty = errors.New("dsql reservoir empty: no connections available")

// ReservoirConfig defines reservoir behavior parameters.
type ReservoirConfig struct {
	TargetReady   int
	LowWatermark  int
	BaseLifetime  time.Duration
	Jitter        time.Duration
	GuardWindow   time.Duration
	InflightLimit int // Max concurrent Open() calls in refiller (default: 8)
}

// LeaseManager provides global connection-count lease acquisition/release.
type LeaseManager interface {
	Acquire(ctx context.Context) (string, error)
	Release(ctx context.Context, leaseID string) error
}

// reservoirDriver implements driver.Driver and sources connections from an in-process reservoir.
// Open() is strictly non-blocking - it either returns a connection immediately or ErrReservoirEmpty.
type reservoirDriver struct {
	res       *Reservoir
	refiller  *reservoirRefiller
	openCount atomic.Int64
	logFunc   LogFunc
	metrics   ReservoirMetrics
}

const reservoirDriverNamePrefix = "pgx-dsql-reservoir-"

var (
	reservoirDriverMu      sync.Mutex
	reservoirDriverCounter int
)

// ReservoirHandle provides access to the reservoir and its refiller for lifecycle management.
// The caller should call Stop() when the database connection is closed to prevent goroutine leaks.
type ReservoirHandle struct {
	Reservoir *Reservoir
	refiller  *reservoirRefiller
}

// Stop stops the refiller goroutines. This should be called when the database connection is closed.
func (h *ReservoirHandle) Stop() {
	if h.refiller != nil {
		h.refiller.Stop()
	}
}

// Len returns the current number of connections in the reservoir.
func (h *ReservoirHandle) Len() int {
	if h.Reservoir != nil {
		return h.Reservoir.Len()
	}
	return 0
}

// RegisterReservoirDriverWithLogger registers a new reservoir-backed driver, starts the refiller loop,
// and returns the unique driver name to pass to sql.Open().
//
// IMPORTANT: Open() never blocks on any global limiter. All potentially blocking work is performed by
// the background refiller.
//
// The returned ReservoirHandle should be used to stop the refiller when the database connection is closed.
// Failure to call Stop() will result in goroutine leaks.
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
	driverName, handle, err := RegisterReservoirDriverWithHandle(username, baseDSN, tokenProvider, rateLimiter, leaseManager, cfg, logFunc, metrics)
	if err != nil {
		return "", nil, err
	}
	return driverName, handle.Reservoir, nil
}

// RegisterReservoirDriverWithHandle is like RegisterReservoirDriverWithLogger but returns a ReservoirHandle
// that allows the caller to stop the refiller goroutines when the database connection is closed.
func RegisterReservoirDriverWithHandle(
	username string,
	baseDSN string,
	tokenProvider TokenProvider,
	rateLimiter RateLimiter,
	leaseManager LeaseManager,
	cfg ReservoirConfig,
	logFunc LogFunc,
	metrics ReservoirMetrics,
) (string, *ReservoirHandle, error) {
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

	wrapper := &reservoirDriver{res: res, refiller: refiller, logFunc: logFunc, metrics: metrics}
	sql.Register(driverName, wrapper)

	handle := &ReservoirHandle{
		Reservoir: res,
		refiller:  refiller,
	}

	return driverName, handle, nil
}

// Open implements driver.Driver.
// Open() is STRICTLY NON-BLOCKING - it either returns a connection immediately or ErrReservoirEmpty.
//
// IMPORTANT: We return ErrReservoirEmpty (not driver.ErrBadConn) when the reservoir is empty.
// This is a critical design decision:
//   - driver.ErrBadConn signals a corrupted connection and triggers DatabaseHandle.ConvertError
//     to recreate the entire connection pool, which would create a new reservoir and leak the
//     old refiller goroutines.
//   - ErrReservoirEmpty signals transient backpressure and should be handled as a retryable
//     error at the persistence layer without triggering pool recreation.
func (d *reservoirDriver) Open(_ string) (driver.Conn, error) {
	start := time.Now()
	openNum := d.openCount.Add(1)
	now := start.UTC()

	// Strictly non-blocking checkout - never wait
	pc, ok := d.res.TryCheckout(now)
	if ok {
		d.metrics.RecordCheckoutLatency(time.Since(start))
		if d.logFunc != nil {
			d.logFunc("Reservoir driver Open() - connection checked out", "open_count", openNum, "reservoir_ready", d.res.Len())
		}
		return newReservoirConn(d.res, pc), nil
	}

	// Reservoir empty - return sentinel error (NOT driver.ErrBadConn)
	// This prevents DatabaseHandle.ConvertError from triggering pool recreation cascade
	d.metrics.RecordCheckoutLatency(time.Since(start))
	d.metrics.IncReservoirEmpty()
	if d.logFunc != nil {
		d.logFunc("Reservoir driver Open() - reservoir empty", "open_count", openNum, "reservoir_ready", 0)
	}
	return nil, ErrReservoirEmpty
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
