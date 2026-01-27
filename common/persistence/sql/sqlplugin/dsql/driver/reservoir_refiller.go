package driver

import (
	"context"
	"database/sql/driver"
	"math/rand"
	"time"
)

// Refiller pacing constants
const (
	// IdleCheckInterval is how often we check the reservoir when at target capacity.
	IdleCheckInterval = 100 * time.Millisecond

	// FailureBackoff is the backoff duration after a connection creation failure.
	FailureBackoff = 250 * time.Millisecond

	// ExpiryScanInterval is how often we scan for and evict expired connections.
	ExpiryScanInterval = 1 * time.Second
)

type reservoirRefiller struct {
	username      string
	baseDSN       string
	res           *Reservoir
	cfg           ReservoirConfig
	tokenProvider TokenProvider
	rateLimiter   RateLimiter
	leaseManager  LeaseManager
	underlying    driver.Driver
	logFunc       LogFunc
	metrics       ReservoirMetrics

	rng   *rand.Rand
	stopC chan struct{}
}

func (r *reservoirRefiller) Start() {
	if r.stopC != nil {
		return
	}
	r.stopC = make(chan struct{})
	if r.rng == nil {
		r.rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if r.metrics == nil {
		r.metrics = &noOpReservoirMetrics{}
	}
	go r.loop()
	go r.expiryScanner()
}

func (r *reservoirRefiller) Stop() {
	if r.stopC != nil {
		close(r.stopC)
	}
}

func (r *reservoirRefiller) loop() {
	// Continuous refill loop - rate limiter is the ONLY throttle.
	//
	// The refiller runs back-to-back openOne() calls whenever the reservoir
	// is below target. The rate limiter in openOne() controls pacing.
	// This ensures the reservoir fills as fast as DSQL allows.
	//
	// Expiry scanning runs in a separate goroutine (expiryScanner) to
	// proactively evict expired connections without waiting for checkouts.

	if r.logFunc != nil {
		r.logFunc("Reservoir refiller started",
			"target_ready", r.cfg.TargetReady,
			"base_lifetime", r.cfg.BaseLifetime.String(),
			"expiry_scan_interval", ExpiryScanInterval.String())
	}

	for {
		select {
		case <-r.stopC:
			return
		default:
		}

		ready := r.res.Len()
		need := r.cfg.TargetReady - ready

		if need <= 0 {
			// At target capacity - brief check interval
			sleepOrStop(r.stopC, IdleCheckInterval)
			continue
		}

		// Create one connection - rate limiter controls pacing
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := r.openOne(ctx)
		cancel()

		if err != nil {
			// Backoff on failure to avoid hammering a failing resource
			sleepOrStop(r.stopC, FailureBackoff)
			continue
		}

		// No delay - immediately try to create next connection
		// Rate limiter is the only throttle
	}
}

// expiryScanner periodically scans the reservoir and evicts expired connections.
// This ensures stale connections don't sit in the reservoir waiting for checkout.
func (r *reservoirRefiller) expiryScanner() {
	if r.logFunc != nil {
		r.logFunc("Reservoir expiry scanner started", "interval", ExpiryScanInterval.String())
	}

	ticker := time.NewTicker(ExpiryScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopC:
			return
		case <-ticker.C:
			now := time.Now().UTC()
			evicted := r.res.ScanAndEvict(now)
			if evicted > 0 && r.logFunc != nil {
				r.logFunc("Reservoir expiry scan evicted connections",
					"evicted", evicted,
					"remaining", r.res.Len(),
					"target", r.cfg.TargetReady)
			}
		}
	}
}

func (r *reservoirRefiller) openOne(ctx context.Context) error {
	// Acquire global conn-count lease first.
	leaseID := ""
	if r.leaseManager != nil {
		id, err := r.leaseManager.Acquire(ctx)
		if err != nil {
			if r.logFunc != nil {
				r.logFunc("Reservoir refiller lease acquire failed", "error", err.Error())
			}
			r.metrics.IncReservoirRefillFailures("lease_acquire")
			return err
		}
		leaseID = id
	}

	// Rate limit connection establishment.
	if r.rateLimiter != nil {
		if err := r.rateLimiter.Wait(ctx); err != nil {
			if r.leaseManager != nil && leaseID != "" {
				_ = r.leaseManager.Release(context.Background(), leaseID)
			}
			r.metrics.IncReservoirRefillFailures("rate_limit")
			return err
		}
	}

	// Get token.
	token, err := r.tokenProvider(ctx)
	if err != nil {
		if r.leaseManager != nil && leaseID != "" {
			_ = r.leaseManager.Release(context.Background(), leaseID)
		}
		r.metrics.IncReservoirRefillFailures("token_provider")
		return err
	}

	// Inject token into DSN.
	dsnWithToken, err := injectToken(r.baseDSN, r.username, token)
	if err != nil {
		if r.leaseManager != nil && leaseID != "" {
			_ = r.leaseManager.Release(context.Background(), leaseID)
		}
		r.metrics.IncReservoirRefillFailures("dsn_inject")
		return err
	}

	conn, err := r.underlying.Open(dsnWithToken)
	if err != nil {
		if r.leaseManager != nil && leaseID != "" {
			_ = r.leaseManager.Release(context.Background(), leaseID)
		}
		r.metrics.IncReservoirRefillFailures("connection_open")
		return err
	}

	// Compute lifetime with jitter.
	// Store CreatedAt and Lifetime separately so remaining lifetime can be
	// computed accurately at checkout time.
	lifetime := r.cfg.BaseLifetime
	if r.cfg.Jitter > 0 {
		lifetime += time.Duration(r.rng.Int63n(int64(r.cfg.Jitter)))
	}
	createdAt := time.Now().UTC()

	r.res.Return(&PhysicalConn{
		Conn:      conn,
		CreatedAt: createdAt,
		Lifetime:  lifetime,
		LeaseID:   leaseID,
	}, createdAt)

	// Emit successful refill metric
	r.metrics.IncReservoirRefills()
	r.metrics.RecordReservoirSize(r.res.Len())

	return nil
}

func sleepOrStop(stop <-chan struct{}, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-stop:
		return
	case <-t.C:
		return
	}
}
