package dsql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/session"
	"go.temporal.io/server/common/resolver"
)

const (
	// PluginName is the name of the plugin
	PluginName = "dsql"
)

var defaultDatabaseNames = []string{
	"postgres",  // normal PostgreSQL default DB name
	"defaultdb", // special behavior for Aiven: #1389
}

type plugin struct {
	driver         driver.Driver
	queryConverter sqlplugin.VisibilityQueryConverter
	retryConfig    RetryConfig

	// Token cache for IAM authentication (lazily initialized)
	tokenCacheMu           sync.Mutex
	tokenCache             *TokenCache
	credentialsProvider    aws.CredentialsProvider
	credentialsInitialized bool

	// Connection rate limiter (lazily initialized)
	// Can be either local (ConnectionRateLimiter) or distributed (DistributedRateLimiter)
	rateLimiterOnce sync.Once
	rateLimiter     driver.RateLimiter

	// Staggered startup (applied once per plugin instance)
	startupDelayOnce sync.Once
}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	persistencesql.RegisterPlugin(PluginName, &plugin{
		driver:         &driver.PGXDriver{},
		queryConverter: &queryConverter{},
		retryConfig:    DefaultRetryConfig(),
	})
}

func (p *plugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return p.queryConverter
}

// reservoirHandleHolder holds a reference to the current reservoir handle.
// This allows the connect closure to stop the old reservoir before creating a new one,
// preventing goroutine leaks when DatabaseHandle.reconnect() is triggered.
type reservoirHandleHolder struct {
	mu     sync.Mutex
	handle *driver.ReservoirHandle
}

func (h *reservoirHandleHolder) Set(handle *driver.ReservoirHandle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Stop the old reservoir before replacing it
	if h.handle != nil {
		h.handle.Stop()
	}
	h.handle = handle
}

func (h *reservoirHandleHolder) Get() *driver.ReservoirHandle {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.handle
}

func (h *reservoirHandleHolder) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.handle != nil {
		h.handle.Stop()
		h.handle = nil
	}
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (sqlplugin.GenericDB, error) {
	// Create a holder for the reservoir handle that persists across reconnects.
	// This allows us to stop the old reservoir when a reconnect happens.
	resHolder := &reservoirHandleHolder{}

	connect := func() (*sqlx.DB, error) {
		if cfg.Connect != nil {
			return cfg.Connect(cfg)
		}
		db, _, reservoirHandle, err := p.createConnectionWithTokenRefreshFull(cfg, r, logger, metricsHandler)
		if err != nil {
			return nil, err
		}
		// Store the new reservoir handle (this also stops the old one if any)
		resHolder.Set(reservoirHandle)
		return db, nil
	}

	handle := sqlplugin.NewDatabaseHandle(dbKind, connect, p.driver.IsConnNeedsRefreshError, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDBWithDependencies(dbKind, cfg.DatabaseName, p.driver, handle, nil, logger, metricsHandler, p.retryConfig)

	// Store the holder in the db so Close() can stop the reservoir
	db.reservoirHolder = resHolder

	return db, nil
}

// createConnectionWithTokenRefreshFull is the full connection creation function that returns
// the reservoir handle for lifecycle management. This is called from the connect closure
// in CreateDB to ensure the reservoir can be stopped on reconnect.
func (p *plugin) createConnectionWithTokenRefreshFull(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*sqlx.DB, *driver.ConnectionRegistry, *driver.ReservoirHandle, error) {
	if cfg.DatabaseName != "" {
		return p.createDSQLConnectionWithAuthFull(cfg, resolver, logger, metricsHandler)
	}

	// database name not provided, try defaults
	defer func() { cfg.DatabaseName = "" }()

	var errors []error
	for _, databaseName := range defaultDatabaseNames {
		cfg.DatabaseName = databaseName
		if db, registry, resHandle, err := p.createDSQLConnectionWithAuthFull(cfg, resolver, logger, metricsHandler); err == nil {
			return db, registry, resHandle, nil
		} else {
			errors = append(errors, err)
		}
	}
	return nil, nil, nil, serviceerror.NewUnavailable(
		fmt.Sprintf("unable to connect to DSQL, tried default DB names: %v, errors: %v", strings.Join(defaultDatabaseNames, ","), errors),
	)
}

// createDSQLConnection creates a DSQL database connection with IAM authentication
func (p *plugin) createDSQLConnection(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*sqlx.DB, error) {
	db, _, _, err := p.createConnectionWithTokenRefreshFull(cfg, resolver, logger, metricsHandler)
	return db, err
}

// createDSQLConnectionWithAuthFull creates a DSQL connection with IAM authentication
// and returns the reservoir handle for lifecycle management.
func (p *plugin) createDSQLConnectionWithAuthFull(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*sqlx.DB, *driver.ConnectionRegistry, *driver.ReservoirHandle, error) {
	// Apply staggered startup delay (only on first connection)
	p.startupDelayOnce.Do(func() {
		if delay := StaggeredStartupDelay(); delay > 0 {
			logger.Info("Applying staggered startup delay for DSQL",
				tag.NewDurationTag("delay", delay))
			time.Sleep(delay)
		}
	})

	// Get region from environment variable (following AWS DSQL sample pattern)
	region := os.Getenv("REGION")
	if region == "" {
		// Fallback to AWS_REGION if REGION is not set
		region = os.Getenv("AWS_REGION")
		if region == "" {
			return nil, nil, nil, fmt.Errorf("REGION or AWS_REGION environment variable must be set for DSQL IAM authentication")
		}
	}

	// Get cluster endpoint from environment variable (following AWS DSQL sample pattern)
	clusterEndpoint := os.Getenv("CLUSTER_ENDPOINT")
	if clusterEndpoint == "" {
		// Extract hostname from ConnectAddr as fallback
		clusterEndpoint = cfg.ConnectAddr
		if strings.Contains(clusterEndpoint, ":") {
			clusterEndpoint = strings.Split(clusterEndpoint, ":")[0]
		}
	}

	// Initialize rate limiter (once per plugin)
	// The rate limiter is passed to the token-refreshing driver so it can
	// rate-limit ALL connection attempts, including pool growth.
	// Can be either:
	// - Token bucket (recommended): Uses DSQL's burst capacity (1000) with 100/sec sustained
	// - Per-second counter: Legacy distributed rate limiter
	// - Local: Per-instance rate limiter (no coordination)
	p.rateLimiterOnce.Do(func() {
		if IsDistributedRateLimiterEnabled() {
			// Use distributed rate limiter backed by DynamoDB
			tableName := GetDistributedRateLimiterTable()
			if tableName == "" {
				logger.Error("Distributed rate limiter enabled but DSQL_DISTRIBUTED_RATE_LIMITER_TABLE not set, falling back to local rate limiter")
				p.rateLimiter = NewConnectionRateLimiter()
				return
			}

			// Create DynamoDB client
			ddbClient, err := p.createDynamoDBClient(context.Background(), region, logger)
			if err != nil {
				logger.Error("Failed to create DynamoDB client for distributed rate limiter, falling back to local rate limiter",
					tag.Error(err))
				p.rateLimiter = NewConnectionRateLimiter()
				return
			}

			// Check if token bucket mode is enabled (recommended)
			if IsTokenBucketEnabled() {
				p.rateLimiter = NewTokenBucketLimiter(ddbClient, tableName, clusterEndpoint, logger)
				logger.Info("DSQL token bucket rate limiter initialized",
					tag.NewStringTag("table", tableName),
					tag.NewStringTag("endpoint", clusterEndpoint),
					tag.NewInt64("rate_per_sec", int64(getEnvInt(TokenBucketRateEnvVar, DefaultTokenBucketRate))),
					tag.NewInt64("capacity", int64(getEnvInt(TokenBucketCapacityEnvVar, DefaultTokenBucketCapacity))))
			} else {
				// Legacy per-second counter mode
				p.rateLimiter = NewDistributedRateLimiter(ddbClient, tableName, clusterEndpoint)
				logger.Info("DSQL distributed rate limiter initialized (per-second counter)",
					tag.NewStringTag("table", tableName),
					tag.NewStringTag("endpoint", clusterEndpoint),
					tag.NewInt64("limit_per_second", int64(getEnvInt(DistributedRateLimiterLimitEnvVar, DefaultDistributedRateLimiterLimit))))
			}
		} else {
			// Use local rate limiter (per-instance)
			p.rateLimiter = NewConnectionRateLimiter()
			logger.Info("DSQL local connection rate limiter initialized",
				tag.NewInt("rate_limit", getEnvInt(ConnectionRateLimitEnvVar, DefaultConnectionRateLimit)),
				tag.NewInt("burst_limit", getEnvInt(ConnectionBurstLimitEnvVar, DefaultConnectionBurstLimit)))
		}
	})

	// Get or initialize token cache
	tokenCache, err := p.getOrInitTokenCache(context.Background(), region, logger, metricsHandler)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize token cache: %w", err)
	}

	tokenDuration := GetConfiguredTokenDuration()

	logger.Info("Creating DSQL connection with IAM authentication",
		tag.NewStringTag("endpoint", clusterEndpoint),
		tag.NewStringTag("region", region),
		tag.NewStringTag("user", adminUser),
		tag.NewDurationTag("token_duration", tokenDuration))

	// Create a token provider function that the driver will call for each new connection
	tokenProvider := func(ctx context.Context) (string, error) {
		token, err := tokenCache.GetToken(ctx, clusterEndpoint, region, adminUser, tokenDuration)
		if err != nil {
			logger.Error("Failed to get fresh DSQL token for connection",
				tag.Error(err),
				tag.NewStringTag("endpoint", clusterEndpoint))
			return "", err
		}
		logger.Debug("Got fresh DSQL token for new connection",
			tag.NewStringTag("endpoint", clusterEndpoint))
		return token, nil
	}

	// Create the connection using the token-refreshing driver
	// Return the reservoir handle so the caller can stop it on reconnect
	db, registry, resHandle, err := p.createConnectionWithTokenRefresh(cfg, resolver, tokenProvider, logger, metricsHandler)
	if err != nil {
		return nil, nil, nil, err
	}

	logger.Info("DSQL connection established successfully with token-refreshing driver",
		tag.NewStringTag("endpoint", clusterEndpoint))

	return db, registry, resHandle, nil
}

// createConnectionWithTokenRefresh creates a database connection using a custom driver
// that refreshes IAM tokens before each new connection.
// Returns the database connection, the connection registry for lifecycle tracking, and the reservoir handle (if reservoir mode).
func (p *plugin) createConnectionWithTokenRefresh(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	tokenProvider driver.TokenProvider,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*sqlx.DB, *driver.ConnectionRegistry, *driver.ReservoirHandle, error) {
	// Build the base DSN (with a placeholder password that will be replaced)
	dsqlConfig := *cfg
	dsqlConfig.User = adminUser
	dsqlConfig.Password = "placeholder" // Will be replaced by tokenProvider

	// Use session to build the DSN
	baseDSN, err := session.BuildDSN(&dsqlConfig, resolver)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to build DSN: %w", err)
	}

	// Register a token-refreshing driver for this connection with logging
	logFunc := func(msg string, keysAndValues ...interface{}) {
		// Convert keysAndValues to tags
		tags := make([]tag.Tag, 0, len(keysAndValues)/2)
		for i := 0; i < len(keysAndValues)-1; i += 2 {
			key, ok := keysAndValues[i].(string)
			if !ok {
				continue
			}
			switch v := keysAndValues[i+1].(type) {
			case string:
				tags = append(tags, tag.NewStringTag(key, v))
			case int64:
				tags = append(tags, tag.NewInt64(key, v))
			case int:
				tags = append(tags, tag.NewInt(key, v))
			case error:
				tags = append(tags, tag.Error(v))
			default:
				tags = append(tags, tag.NewStringTag(key, fmt.Sprintf("%v", v)))
			}
		}
		logger.Info(msg, tags...)
	}

	// Reservoir mode: driver.Open() must never block on global limiters.
	// We achieve this by sourcing physical connections from an in-process reservoir
	// refilled by a background goroutine which is the only place that waits on
	// global rate/conn-count limiters.
	if IsReservoirEnabled() {
		maxOpen := cfg.MaxConns
		if maxOpen <= 0 {
			maxOpen = session.DefaultMaxConns
		}
		resCfg := GetReservoirConfig(maxOpen, cfg.PoolSizeHint)

		// Optional distributed conn-count slot block manager.
		// Uses block-based allocation to avoid hot partition issues.
		// Skip for ephemeral pools - they're short-lived and don't need global coordination.
		var leaseMgr driver.LeaseManager
		if IsDistributedConnLeaseEnabled() && cfg.PoolSizeHint != "ephemeral" {
			table := GetDistributedConnLeaseTable()
			if table == "" {
				logger.Error("Distributed conn lease enabled but DSQL_DISTRIBUTED_CONN_LEASE_TABLE not set; proceeding without global conn count limiting")
			} else {
				ddbClient, err := p.createDynamoDBClient(context.Background(), os.Getenv("AWS_REGION"), logger)
				if err != nil {
					logger.Error("Failed to create DynamoDB client for slot block manager; proceeding without global conn count limiting", tag.Error(err))
				} else {
					// Get service name from environment or use a default
					serviceName := os.Getenv("SERVICES")
					if serviceName == "" {
						serviceName = "unknown"
					}

					// Configure slot blocks based on target pool size and global limit
					slotCfg := GetSlotBlockConfig(resCfg.TargetReady)

					// Create slot block metrics
					slotBlockMetrics := NewSlotBlockMetrics(metricsHandler)

					slotMgr, err := NewSlotBlockManager(
						ddbClient,
						table,
						os.Getenv("CLUSTER_ENDPOINT"),
						serviceName,
						slotCfg,
						logger,
						slotBlockMetrics,
					)
					if err != nil {
						logger.Error("Failed to initialize slot block manager; proceeding without global conn count limiting", tag.Error(err))
					} else {
						// Acquire enough slots for our target pool size
						acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 30*time.Second)
						acquiredSlots, err := slotMgr.AcquireSlots(acquireCtx, resCfg.TargetReady)
						acquireCancel()

						if err != nil {
							logger.Error("Failed to acquire slot blocks; proceeding without global conn count limiting",
								tag.Error(err),
								tag.NewInt("acquired_slots", acquiredSlots),
								tag.NewInt("target_slots", resCfg.TargetReady))
						} else if acquiredSlots < resCfg.TargetReady {
							logger.Warn("Acquired fewer slots than target; may not reach full pool size",
								tag.NewInt("acquired_slots", acquiredSlots),
								tag.NewInt("target_slots", resCfg.TargetReady))
							leaseMgr = slotMgr
						} else {
							logger.Info("Slot block manager initialized",
								tag.NewInt("acquired_slots", acquiredSlots),
								tag.NewInt("target_slots", resCfg.TargetReady))
							leaseMgr = slotMgr
						}
					}
				}
			}
		}

		// Create reservoir metrics using the metrics handler
		reservoirMetrics := NewReservoirMetrics(metricsHandler)

		driverName, reservoirHandle, err := driver.RegisterReservoirDriverWithHandle(
			adminUser,
			baseDSN,
			tokenProvider,
			p.rateLimiter,
			leaseMgr,
			driver.ReservoirConfig{
				TargetReady:   resCfg.TargetReady,
				LowWatermark:  resCfg.LowWatermark,
				BaseLifetime:  resCfg.BaseLifetime,
				Jitter:        resCfg.Jitter,
				GuardWindow:   resCfg.GuardWindow,
				InflightLimit: resCfg.InflightLimit,
			},
			logFunc,
			reservoirMetrics,
		)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to register reservoir driver: %w", err)
		}

		logger.Info("Registered reservoir-backed driver for DSQL",
			tag.NewStringTag("driver_name", driverName),
			tag.NewStringTag("pool_size_hint", cfg.PoolSizeHint),
			tag.NewInt("target_ready", resCfg.TargetReady))

		// Wait for reservoir to reach low watermark before accepting requests.
		// This ensures the service has connections available when it starts handling traffic.
		logger.Info("Waiting for reservoir initial fill",
			tag.NewInt("target", resCfg.LowWatermark),
			tag.NewDurationTag("timeout", resCfg.InitialFillTimeout),
			tag.NewStringTag("pool_size_hint", cfg.PoolSizeHint))

		fillCtx, fillCancel := context.WithTimeout(context.Background(), resCfg.InitialFillTimeout)
		defer fillCancel()

		fillStart := time.Now()
		timedOut := false
	fillLoop:
		for reservoirHandle.Len() < resCfg.LowWatermark {
			select {
			case <-fillCtx.Done():
				timedOut = true
				break fillLoop
			case <-time.After(100 * time.Millisecond):
				// Check again
			}
		}

		fillDuration := time.Since(fillStart)
		currentSize := reservoirHandle.Len()

		if timedOut {
			logger.Warn("Reservoir initial fill timeout - proceeding with partial fill",
				tag.NewInt("current", currentSize),
				tag.NewInt("target", resCfg.LowWatermark),
				tag.NewDurationTag("elapsed", fillDuration),
				tag.NewStringTag("pool_size_hint", cfg.PoolSizeHint))
		} else {
			logger.Info("Reservoir initial fill complete",
				tag.NewInt("current", currentSize),
				tag.NewInt("target", resCfg.LowWatermark),
				tag.NewDurationTag("elapsed", fillDuration),
				tag.NewStringTag("pool_size_hint", cfg.PoolSizeHint))
		}

		sqlDB, err := sql.Open(driverName, baseDSN)
		if err != nil {
			reservoirHandle.Stop() // Clean up refiller on error
			return nil, nil, nil, fmt.Errorf("failed to open DSQL connection (reservoir): %w", err)
		}
		db := sqlx.NewDb(sqlDB, "pgx")

		// Pool settings: database/sql pool acts as borrower/returner only.
		if cfg.MaxConns > 0 {
			db.SetMaxOpenConns(cfg.MaxConns)
		} else {
			db.SetMaxOpenConns(session.DefaultMaxConns)
		}
		if cfg.MaxIdleConns > 0 {
			db.SetMaxIdleConns(cfg.MaxIdleConns)
		} else {
			db.SetMaxIdleConns(session.DefaultMaxIdleConns)
		}
		// Disable database/sql driven lifetime/idle closures when using reservoir.
		// The reservoir manages connection lifecycle internally.
		db.SetConnMaxLifetime(0)
		db.SetConnMaxIdleTime(0)
		db.MapperFunc(strcase.ToSnake)

		if err := db.Ping(); err != nil {
			db.Close()
			reservoirHandle.Stop() // Clean up refiller on error
			return nil, nil, nil, fmt.Errorf("failed to ping DSQL (reservoir): %w", err)
		}

		logger.Info("DSQL reservoir mode enabled",
			tag.NewInt("reservoir_target_ready", resCfg.TargetReady),
			tag.NewInt("reservoir_low_watermark", resCfg.LowWatermark),
			tag.NewDurationTag("reservoir_base_lifetime", resCfg.BaseLifetime),
			tag.NewDurationTag("reservoir_lifetime_jitter", resCfg.Jitter),
			tag.NewDurationTag("reservoir_guard_window", resCfg.GuardWindow),
			tag.NewDurationTag("reservoir_initial_fill_timeout", resCfg.InitialFillTimeout),
			tag.NewStringTag("pool_size_hint", cfg.PoolSizeHint),
		)

		// Return reservoir handle for lifecycle management
		return db, nil, reservoirHandle, nil
	}

	// Non-reservoir mode: use token-refreshing driver with pool warmup and keeper
	driverName, registry, err := driver.RegisterTokenRefreshingDriverWithLogger(adminUser, tokenProvider, p.rateLimiter, logFunc)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to register token-refreshing driver: %w", err)
	}

	logger.Info("Registered token-refreshing driver for DSQL",
		tag.NewStringTag("driver_name", driverName))

	// Open connection using the token-refreshing driver
	// The driver will call tokenProvider to get a fresh token before each connection
	sqlDB, err := sql.Open(driverName, baseDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to open DSQL connection: %w", err)
	}

	// Wrap with sqlx using "pgx" as the driver name for correct bindvar type ($1, $2, etc.)
	// This is critical - sqlx needs to know it's PostgreSQL-compatible for named parameters
	db := sqlx.NewDb(sqlDB, "pgx")

	// Apply pool settings
	targetPoolSize := session.DefaultMaxConns
	if cfg.MaxConns > 0 {
		targetPoolSize = cfg.MaxConns
		db.SetMaxOpenConns(cfg.MaxConns)
	} else {
		db.SetMaxOpenConns(session.DefaultMaxConns)
	}

	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	} else {
		db.SetMaxIdleConns(session.DefaultMaxIdleConns)
	}

	// Set MaxConnLifetime - Go will close connections older than this.
	// Pool Keeper will refill to maintain pool size.
	maxConnLifetime := session.DefaultMaxConnLifetime
	if cfg.MaxConnLifetime > 0 {
		maxConnLifetime = cfg.MaxConnLifetime
	}
	db.SetConnMaxLifetime(maxConnLifetime)

	// Set idle time - 0 means connections never expire due to idle time.
	// This is critical for DSQL: pool must stay at max size always.
	db.SetConnMaxIdleTime(session.DefaultMaxConnIdleTime)

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)

	// Verify connection works
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, nil, nil, fmt.Errorf("failed to ping DSQL: %w", err)
	}

	// Log pool configuration for debugging
	logger.Info("DSQL connection pool configured",
		tag.NewInt("max_open_conns", db.Stats().MaxOpenConnections),
		tag.NewInt("current_open", db.Stats().OpenConnections),
		tag.NewInt("cfg_max_conns", cfg.MaxConns),
		tag.NewInt("cfg_max_idle_conns", cfg.MaxIdleConns),
		tag.NewDurationTag("max_conn_lifetime", maxConnLifetime),
		tag.NewDurationTag("max_conn_idle_time", session.DefaultMaxConnIdleTime))

	// Pre-warm the connection pool with staggered delays.
	// Staggering ensures connections hit MaxConnLifetime at different times,
	// avoiding thundering herd when connections expire.
	// For large pools, stagger duration is calculated to respect DSQL rate limits.
	warmupCfg := DefaultPoolWarmupConfig()
	warmupCfg.TargetConnections = targetPoolSize
	warmupCfg.StaggerDuration = CalculateStaggerDuration(targetPoolSize)

	logger.Info("Starting DSQL connection pool warmup",
		tag.NewInt("target_connections", targetPoolSize),
		tag.NewDurationTag("stagger_duration", warmupCfg.StaggerDuration))

	warmupResult, err := WarmupPool(context.Background(), db.DB, warmupCfg, logger)
	if err != nil {
		logger.Warn("DSQL pool warmup failed", tag.Error(err))
		// Don't fail - warmup is best-effort
	}

	// Close warmed connections to return them to the pool
	if warmupResult != nil {
		for _, conn := range warmupResult.Connections {
			conn.Close() // Returns to pool, doesn't close TCP
		}
		logger.Info("DSQL pool warmup complete, connections returned to pool",
			tag.NewInt("warmed_connections", len(warmupResult.Connections)))
	}

	// Start the Pool Keeper to maintain pool size.
	// Go's MaxConnLifetime closes old connections; Keeper refills to target.
	// MaxConnsPerTick is scaled based on pool size to handle peak expiry rates.
	keeperCfg := DefaultPoolKeeperConfig(targetPoolSize)
	keeper := NewPoolKeeper(db.DB, keeperCfg, logger, metricsHandler)

	logger.Info("DSQL pool keeper initialized",
		tag.NewInt("target_pool_size", targetPoolSize),
		tag.NewDurationTag("tick_interval", keeperCfg.TickInterval),
		tag.NewInt("max_conns_per_tick", keeperCfg.MaxConnsPerTick),
		tag.NewDurationTag("max_conn_lifetime", maxConnLifetime))

	// Start the background keeper goroutine
	keeper.Start(context.Background())

	// Return nil for reservoir handle - non-reservoir mode doesn't use it
	return db, registry, nil, nil
}

// getOrInitTokenCache returns the token cache, initializing it if necessary.
// Uses double-checked locking for thread safety.
func (p *plugin) getOrInitTokenCache(
	ctx context.Context,
	region string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*TokenCache, error) {
	// Fast path: already initialized
	if p.tokenCache != nil && p.credentialsInitialized {
		return p.tokenCache, nil
	}

	// Slow path: initialize with lock
	p.tokenCacheMu.Lock()
	defer p.tokenCacheMu.Unlock()

	// Double-check after acquiring lock
	if p.tokenCache != nil && p.credentialsInitialized {
		return p.tokenCache, nil
	}

	// Resolve credentials once
	credentialsProvider, err := ResolveCredentialsProvider(ctx, region)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve AWS credentials: %w", err)
	}

	p.credentialsProvider = credentialsProvider
	p.tokenCache = NewTokenCache(credentialsProvider, logger, metricsHandler)
	p.credentialsInitialized = true

	logger.Info("DSQL token cache initialized",
		tag.NewStringTag("region", region))

	return p.tokenCache, nil
}

// createDynamoDBClient creates a DynamoDB client for the distributed rate limiter.
func (p *plugin) createDynamoDBClient(
	ctx context.Context,
	region string,
	logger log.Logger,
) (*dynamodb.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	logger.Info("Created DynamoDB client for distributed rate limiter",
		tag.NewStringTag("region", region))

	return client, nil
}
