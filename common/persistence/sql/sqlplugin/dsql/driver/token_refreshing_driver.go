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

// LogFunc is a callback for logging driver events.
type LogFunc func(msg string, keysAndValues ...interface{})

// tokenRefreshingDriver wraps the pgx stdlib driver to inject fresh IAM tokens
// before each connection. This is necessary because database/sql doesn't support
// dynamic credentials - the DSN is fixed at pool creation time.
//
// When sqlx/database/sql needs a new connection (due to MaxConnLifetime, pool growth,
// or connection failure), it calls driver.Open(dsn). This wrapper intercepts that call,
// gets a fresh token from the TokenProvider, and modifies the DSN before passing it
// to the underlying pgx driver.
type tokenRefreshingDriver struct {
	underlying    driver.Driver
	tokenProvider TokenProvider
	username      string
	driverName    string
	logFunc       LogFunc
	openCount     atomic.Int64
	mu            sync.RWMutex
}

// driverName is the name used to register this driver with database/sql.
// Each DSQL connection gets its own unique driver registration.
const driverNamePrefix = "pgx-dsql-"

var (
	driverCounter int
	driverMu      sync.Mutex
)

// RegisterTokenRefreshingDriver registers a new token-refreshing driver and returns
// the driver name to use with sql.Open(). Each call creates a unique driver registration.
//
// The username is used when constructing the DSN with fresh tokens.
func RegisterTokenRefreshingDriver(username string, tokenProvider TokenProvider) (string, error) {
	return RegisterTokenRefreshingDriverWithLogger(username, tokenProvider, nil)
}

// RegisterTokenRefreshingDriverWithLogger registers a new token-refreshing driver with logging support.
func RegisterTokenRefreshingDriverWithLogger(username string, tokenProvider TokenProvider, logFunc LogFunc) (string, error) {
	if tokenProvider == nil {
		return "", fmt.Errorf("tokenProvider cannot be nil")
	}
	if username == "" {
		username = "admin"
	}

	// Generate unique driver name
	driverMu.Lock()
	driverCounter++
	driverName := fmt.Sprintf("%s%d", driverNamePrefix, driverCounter)
	driverMu.Unlock()

	// Create and register the wrapper driver
	wrapper := &tokenRefreshingDriver{
		underlying:    stdlib.GetDefaultDriver(),
		tokenProvider: tokenProvider,
		username:      username,
		driverName:    driverName,
		logFunc:       logFunc,
	}

	sql.Register(driverName, wrapper)
	return driverName, nil
}

// Open implements driver.Driver. It gets a fresh token and injects it into the DSN
// before opening the connection with the underlying pgx driver.
func (d *tokenRefreshingDriver) Open(dsn string) (driver.Conn, error) {
	openNum := d.openCount.Add(1)

	// Log that the token-refreshing driver is being used
	if d.logFunc != nil {
		d.logFunc("Token-refreshing driver Open() called - getting fresh token",
			"driver_name", d.driverName,
			"open_count", openNum)
	}

	// Get fresh token
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	token, err := d.tokenProvider(ctx)
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

	if d.logFunc != nil {
		d.logFunc("Token-refreshing driver connection established with fresh token",
			"driver_name", d.driverName,
			"open_count", openNum)
	}

	return conn, nil
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
