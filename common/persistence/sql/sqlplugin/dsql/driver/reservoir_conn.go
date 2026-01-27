package driver

import (
	"context"
	"database/sql/driver"
	"sync/atomic"
	"time"
)

// reservoirConn wraps a PhysicalConn and returns it to the Reservoir on Close.
//
// database/sql expects driver.Conn to be exclusively owned by the pool while in use.
// Returning the physical connection to the Reservoir on Close is safe because the pool
// will no longer use this wrapper instance after Close.
type reservoirConn struct {
	r  *Reservoir
	pc *PhysicalConn

	closed atomic.Bool
	bad    atomic.Bool
}

func newReservoirConn(r *Reservoir, pc *PhysicalConn) *reservoirConn {
	return &reservoirConn{r: r, pc: pc}
}

// Prepare implements driver.Conn.
func (c *reservoirConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.pc.Conn.Prepare(query)
	if err == driver.ErrBadConn {
		c.bad.Store(true)
	}
	return stmt, err
}

// Close implements driver.Conn.
func (c *reservoirConn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	if c.r == nil || c.pc == nil {
		return nil
	}
	now := time.Now().UTC()
	if c.bad.Load() {
		// Discard physical connection by setting lifetime to 0.
		// This ensures RemainingLifetime() returns 0 and the connection is discarded.
		c.pc.Lifetime = 0
	}
	c.r.Return(c.pc, now)
	c.pc = nil
	return nil
}

// Begin implements driver.Conn.
func (c *reservoirConn) Begin() (driver.Tx, error) {
	tx, err := c.pc.Conn.Begin()
	if err == driver.ErrBadConn {
		c.bad.Store(true)
	}
	return tx, err
}

// Optional interface forwarders

func (c *reservoirConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if v, ok := c.pc.Conn.(driver.ConnBeginTx); ok {
		tx, err := v.BeginTx(ctx, opts)
		if err == driver.ErrBadConn {
			c.bad.Store(true)
		}
		return tx, err
	}
	// Fallback to Begin if not supported
	return c.Begin()
}

func (c *reservoirConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if v, ok := c.pc.Conn.(driver.ExecerContext); ok {
		res, err := v.ExecContext(ctx, query, args)
		if err == driver.ErrBadConn {
			c.bad.Store(true)
		}
		return res, err
	}
	return nil, driver.ErrSkip
}

func (c *reservoirConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if v, ok := c.pc.Conn.(driver.QueryerContext); ok {
		rows, err := v.QueryContext(ctx, query, args)
		if err == driver.ErrBadConn {
			c.bad.Store(true)
		}
		return rows, err
	}
	return nil, driver.ErrSkip
}

func (c *reservoirConn) Ping(ctx context.Context) error {
	if v, ok := c.pc.Conn.(driver.Pinger); ok {
		err := v.Ping(ctx)
		if err == driver.ErrBadConn {
			c.bad.Store(true)
		}
		return err
	}
	return nil
}

func (c *reservoirConn) ResetSession(ctx context.Context) error {
	if v, ok := c.pc.Conn.(driver.SessionResetter); ok {
		err := v.ResetSession(ctx)
		if err == driver.ErrBadConn {
			c.bad.Store(true)
		}
		return err
	}
	return nil
}

func (c *reservoirConn) IsValid() bool {
	if v, ok := c.pc.Conn.(driver.Validator); ok {
		okv := v.IsValid()
		if !okv {
			c.bad.Store(true)
		}
		return okv
	}
	return true
}
