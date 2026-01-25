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

// TokenProvider is a function that returns a fresh IAM token for DSQL authentication.
// It is called before each new connection is established.
type TokenProvider func(ctx context.Context) (string, error)

// RateLimiter is an interface for connection rate limiting.
// This allows the driver to rate-limit ALL connection attempts, including
// pool growth initiated by database/sql internally.
type RateLimiter interface {
	// Wait blocks until a connection can be established within rate limits.
	Wait(ctx context.Context) error
}

// LogFunc is a callback for logging driver events.
type LogFunc func(msg string, keysAndValues ...any)

// tokenRefreshingDriver wraps the pgx stdlib driver to inject fresh IAM tokens
// before each connection. This is necessary because database/sql doesn't support
// dynamic credentials - the DSN is fixed at pool creation time.
//
// When sqlx/database/sql needs a new connection (due to MaxConnLifetime, pool growth,
// or connection failure), it calls driver.Open(dsn). This wrapper intercepts that call,
// gets a fresh token from the TokenProvider, and modifies the DSN before passing it
// to the underlying pgx driver.
//
// The driver also enforces connection rate limiting to respect DSQL's cluster-wide
// connection rate limits (100 connections/sec). This is critical because pool growth
// happens internally in database/sql and would otherwise bypass any rate limiting
// applied at the application level.
//
// Additionally, the driver tracks all connection birth times in a registry, enabling
// the connection refresher to know when connections should be replaced.
type tokenRefreshingDriver struct {
	underlying    driver.Driver
	tokenProvider TokenProvider
	rateLimiter   RateLimiter         // Rate limiter for connection establishment
	registry      *ConnectionRegistry // Tracks connection birth times
	username      string
	driverName    string
	logFunc       LogFunc
	openCount     atomic.Int64
	mu            sync.RWMutex
}

// trackedConn wraps a driver.Conn to track when it's closed.
type trackedConn struct {
	driver.Conn
	id       uint64
	registry *ConnectionRegistry
	logFunc  LogFunc
}

// driverName is the name used to register this driver with database/sql.
// Each DSQL connection gets its own unique driver registration.
const driverNamePrefix = "pgx-dsql-"

var (
	driverCounter int
	driverMu      sync.Mutex
)

// RegisterTokenRefreshingDriver registers a new token-refreshing driver and returns
// the driver name to use with sql.Open() and the connection registry.
// Each call creates a unique driver registration.
//
// The username is used when constructing the DSN with fresh tokens.
func RegisterTokenRefreshingDriver(username string, tokenProvider TokenProvider) (string, *ConnectionRegistry, error) {
	return RegisterTokenRefreshingDriverWithLogger(username, tokenProvider, nil, nil)
}

// RegisterTokenRefreshingDriverWithLogger registers a new token-refreshing driver with logging
// and rate limiting support. Returns the driver name and the connection registry.
//
// The rateLimiter parameter is optional but strongly recommended for DSQL connections.
// When provided, it ensures ALL connection attempts (including pool growth) respect
// DSQL's cluster-wide connection rate limits (100 connections/sec).
//
// The returned ConnectionRegistry can be used by the connection refresher to track
// connection ages and trigger replacement of old connections.
func RegisterTokenRefreshingDriverWithLogger(username string, tokenProvider TokenProvider, rateLimiter RateLimiter, logFunc LogFunc) (string, *ConnectionRegistry, error) {
	if tokenProvider == nil {
		return "", nil, fmt.Errorf("tokenProvider cannot be nil")
	}
	if username == "" {
		username = "admin"
	}

	// Generate unique driver name
	driverMu.Lock()
	driverCounter++
	driverName := fmt.Sprintf("%s%d", driverNamePrefix, driverCounter)
	driverMu.Unlock()

	// Create connection registry
	registry := NewConnectionRegistry()

	// Create and register the wrapper driver
	wrapper := &tokenRefreshingDriver{
		underlying:    stdlib.GetDefaultDriver(),
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		registry:      registry,
		username:      username,
		driverName:    driverName,
		logFunc:       logFunc,
	}

	sql.Register(driverName, wrapper)
	return driverName, registry, nil
}

// Open implements driver.Driver. It enforces rate limiting, gets a fresh token,
// and injects it into the DSN before opening the connection with the underlying pgx driver.
//
// Rate limiting is applied here (not just at pool creation) because database/sql
// calls Open() directly when growing the pool, bypassing any application-level rate limiting.
func (d *tokenRefreshingDriver) Open(dsn string) (driver.Conn, error) {
	openNum := d.openCount.Add(1)

	// Create context for this connection attempt
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Apply rate limiting BEFORE attempting connection
	// This is critical for DSQL which has cluster-wide connection rate limits
	if d.rateLimiter != nil {
		if d.logFunc != nil {
			d.logFunc("Token-refreshing driver waiting for rate limiter",
				"driver_name", d.driverName,
				"open_count", openNum)
		}
		if err := d.rateLimiter.Wait(ctx); err != nil {
			if d.logFunc != nil {
				d.logFunc("Token-refreshing driver rate limit wait failed",
					"driver_name", d.driverName,
					"open_count", openNum,
					"error", err.Error())
			}
			return nil, fmt.Errorf("connection rate limit exceeded: %w", err)
		}
	}

	// Log that the token-refreshing driver is being used
	if d.logFunc != nil {
		d.logFunc("Token-refreshing driver Open() called - getting fresh token",
			"driver_name", d.driverName,
			"open_count", openNum)
	}

	// Get fresh token (use a shorter timeout since we already waited for rate limiter)
	tokenCtx, tokenCancel := context.WithTimeout(ctx, 10*time.Second)
	defer tokenCancel()

	token, err := d.tokenProvider(tokenCtx)
	if err != nil {
		if d.logFunc != nil {
			d.logFunc("Token-refreshing driver failed to get token",
				"driver_name", d.driverName,
				"error", err.Error())
		}
		return nil, fmt.Errorf("failed to get fresh DSQL token: %w", err)
	}

	// Inject token into DSN
	dsnWithToken, err := d.injectToken(dsn, token)
	if err != nil {
		return nil, fmt.Errorf("failed to inject token into DSN: %w", err)
	}

	// Open connection with fresh token
	conn, err := d.underlying.Open(dsnWithToken)
	if err != nil {
		if d.logFunc != nil {
			d.logFunc("Token-refreshing driver connection failed",
				"driver_name", d.driverName,
				"open_count", openNum,
				"error", err.Error())
		}
		return nil, err
	}

	// Register connection in the registry for lifecycle tracking
	tracked := d.registry.Register(conn)

	if d.logFunc != nil {
		d.logFunc("Token-refreshing driver connection established with fresh token",
			"driver_name", d.driverName,
			"open_count", openNum,
			"connection_id", tracked.ID)
	}

	// Wrap the connection to track when it's closed
	return &trackedConn{
		Conn:     conn,
		id:       tracked.ID,
		registry: d.registry,
		logFunc:  d.logFunc,
	}, nil
}

// Close implements driver.Conn. It unregisters the connection from the registry.
func (c *trackedConn) Close() error {
	c.registry.Unregister(c.id)
	if c.logFunc != nil {
		c.logFunc("Token-refreshing driver connection closed",
			"connection_id", c.id)
	}
	return c.Conn.Close()
}

// injectToken creates a new DSN with the provided token as the password.
func (d *tokenRefreshingDriver) injectToken(dsn, token string) (string, error) {
	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("failed to parse DSN: %w", err)
	}

	// Get username from existing DSN or use configured default
	d.mu.RLock()
	username := d.username
	d.mu.RUnlock()

	if parsedDSN.User != nil && parsedDSN.User.Username() != "" {
		username = parsedDSN.User.Username()
	}

	// Set new password (token)
	parsedDSN.User = url.UserPassword(username, token)

	return parsedDSN.String(), nil
}
