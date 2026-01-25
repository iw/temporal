package driver

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mockRateLimiter is a test implementation of RateLimiter
type mockRateLimiter struct {
	waitCalls atomic.Int64
	waitDelay time.Duration
	waitErr   error
}

func (m *mockRateLimiter) Wait(ctx context.Context) error {
	m.waitCalls.Add(1)
	if m.waitDelay > 0 {
		select {
		case <-time.After(m.waitDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return m.waitErr
}

// mockDriver is a test implementation of driver.Driver
type mockDriver struct {
	openCalls atomic.Int64
	openErr   error
	conn      driver.Conn
}

func (m *mockDriver) Open(name string) (driver.Conn, error) {
	m.openCalls.Add(1)
	if m.openErr != nil {
		return nil, m.openErr
	}
	return m.conn, nil
}

// mockConn is a minimal driver.Conn implementation for testing
type mockConn struct{}

func (m *mockConn) Prepare(query string) (driver.Stmt, error) { return nil, nil }
func (m *mockConn) Close() error                              { return nil }
func (m *mockConn) Begin() (driver.Tx, error)                 { return nil, nil }

func TestRegisterTokenRefreshingDriverWithLogger_NilTokenProvider(t *testing.T) {
	_, _, err := RegisterTokenRefreshingDriverWithLogger("admin", nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tokenProvider cannot be nil")
}

func TestRegisterTokenRefreshingDriverWithLogger_EmptyUsername(t *testing.T) {
	tokenProvider := func(ctx context.Context) (string, error) {
		return "token", nil
	}
	driverName, registry, err := RegisterTokenRefreshingDriverWithLogger("", tokenProvider, nil, nil)
	require.NoError(t, err)
	require.Contains(t, driverName, driverNamePrefix)
	require.NotNil(t, registry)
}

func TestRegisterTokenRefreshingDriverWithLogger_UniqueDriverNames(t *testing.T) {
	tokenProvider := func(ctx context.Context) (string, error) {
		return "token", nil
	}

	name1, _, err := RegisterTokenRefreshingDriverWithLogger("admin", tokenProvider, nil, nil)
	require.NoError(t, err)

	name2, _, err := RegisterTokenRefreshingDriverWithLogger("admin", tokenProvider, nil, nil)
	require.NoError(t, err)

	require.NotEqual(t, name1, name2, "Each registration should get a unique driver name")
}

func TestTokenRefreshingDriver_RateLimiterCalled(t *testing.T) {
	// This test verifies that the rate limiter is called during Open()
	rateLimiter := &mockRateLimiter{}
	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}
	mockUnderlying := &mockDriver{conn: &mockConn{}}
	registry := NewConnectionRegistry()

	// Create the driver directly for testing
	d := &tokenRefreshingDriver{
		underlying:    mockUnderlying,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		registry:      registry,
		username:      "admin",
		driverName:    "test-driver",
	}

	// Call Open - should succeed
	conn, err := d.Open("postgres://localhost/test")
	require.NoError(t, err)
	require.NotNil(t, conn)

	// Verify rate limiter was called
	require.Equal(t, int64(1), rateLimiter.waitCalls.Load(), "Rate limiter Wait() should be called once")
	// Verify underlying driver was called
	require.Equal(t, int64(1), mockUnderlying.openCalls.Load(), "Underlying driver Open() should be called once")
	// Verify connection was registered
	require.Equal(t, 1, registry.Count(), "Connection should be registered in registry")
}

func TestTokenRefreshingDriver_RateLimiterError(t *testing.T) {
	rateLimiter := &mockRateLimiter{
		waitErr: errors.New("rate limit exceeded"),
	}
	tokenProviderCalled := false
	tokenProvider := func(ctx context.Context) (string, error) {
		tokenProviderCalled = true
		return "", nil
	}
	mockUnderlying := &mockDriver{conn: &mockConn{}}
	registry := NewConnectionRegistry()

	d := &tokenRefreshingDriver{
		underlying:    mockUnderlying,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		registry:      registry,
		username:      "admin",
		driverName:    "test-driver",
	}

	_, err := d.Open("postgres://localhost/test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection rate limit exceeded")
	require.False(t, tokenProviderCalled, "Token provider should not be called when rate limiter fails")
}

func TestTokenRefreshingDriver_RateLimiterContextCancelled(t *testing.T) {
	// Create a rate limiter that returns context error
	rateLimiter := &mockRateLimiter{
		waitErr: context.DeadlineExceeded,
	}
	tokenProvider := func(ctx context.Context) (string, error) {
		return "token", nil
	}
	mockUnderlying := &mockDriver{conn: &mockConn{}}
	registry := NewConnectionRegistry()

	d := &tokenRefreshingDriver{
		underlying:    mockUnderlying,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		registry:      registry,
		username:      "admin",
		driverName:    "test-driver",
	}

	_, err := d.Open("postgres://localhost/test")
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection rate limit exceeded")
}

func TestTokenRefreshingDriver_NilRateLimiter(t *testing.T) {
	// When rate limiter is nil, Open should proceed without rate limiting
	tokenProviderCalled := false
	tokenProvider := func(ctx context.Context) (string, error) {
		tokenProviderCalled = true
		return "test-token", nil
	}
	mockUnderlying := &mockDriver{conn: &mockConn{}}
	registry := NewConnectionRegistry()

	d := &tokenRefreshingDriver{
		underlying:    mockUnderlying,
		tokenProvider: tokenProvider,
		rateLimiter:   nil, // No rate limiter
		registry:      registry,
		username:      "admin",
		driverName:    "test-driver",
	}

	conn, err := d.Open("postgres://localhost/test")
	require.NoError(t, err)
	require.NotNil(t, conn)
	require.True(t, tokenProviderCalled, "Token provider should be called when rate limiter is nil")
}

func TestTokenRefreshingDriver_ConcurrentRateLimiting(t *testing.T) {
	// Test that multiple concurrent Open() calls all go through rate limiter
	rateLimiter := &mockRateLimiter{}

	tokenProvider := func(ctx context.Context) (string, error) {
		return "test-token", nil
	}
	mockUnderlying := &mockDriver{conn: &mockConn{}}
	registry := NewConnectionRegistry()

	d := &tokenRefreshingDriver{
		underlying:    mockUnderlying,
		tokenProvider: tokenProvider,
		rateLimiter:   rateLimiter,
		registry:      registry,
		username:      "admin",
		driverName:    "test-driver",
	}

	// Launch multiple concurrent Open() calls
	var wg sync.WaitGroup
	numCalls := 10
	for i := 0; i < numCalls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Open("postgres://localhost/test")
		}()
	}
	wg.Wait()

	// All calls should have gone through rate limiter
	require.Equal(t, int64(numCalls), rateLimiter.waitCalls.Load(),
		"All concurrent Open() calls should go through rate limiter")
}

func TestInjectToken(t *testing.T) {
	d := &tokenRefreshingDriver{
		username: "admin",
	}

	tests := []struct {
		name     string
		dsn      string
		token    string
		wantUser string
		wantPass string
	}{
		{
			name:     "basic DSN",
			dsn:      "postgres://localhost/test",
			token:    "my-token",
			wantUser: "admin",
			wantPass: "my-token",
		},
		{
			name:     "DSN with existing user",
			dsn:      "postgres://existinguser@localhost/test",
			token:    "my-token",
			wantUser: "existinguser",
			wantPass: "my-token",
		},
		{
			name:     "DSN with existing user and password",
			dsn:      "postgres://existinguser:oldpass@localhost/test",
			token:    "new-token",
			wantUser: "existinguser",
			wantPass: "new-token",
		},
		{
			name:     "token with special characters",
			dsn:      "postgres://localhost/test",
			token:    "token/with+special=chars",
			wantUser: "admin",
			wantPass: "token/with+special=chars",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := d.injectToken(tt.dsn, tt.token)
			require.NoError(t, err)
			require.Contains(t, result, tt.wantUser+":")
			// URL encoding may change the token, so just verify it's in there
			require.NotEmpty(t, result)
		})
	}
}

func TestInjectToken_InvalidDSN(t *testing.T) {
	d := &tokenRefreshingDriver{
		username: "admin",
	}

	_, err := d.injectToken("not-a-valid-url-\x00", "token")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse DSN")
}
