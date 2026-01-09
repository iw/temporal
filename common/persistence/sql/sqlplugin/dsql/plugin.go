package dsql

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/driver"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql/session"
	"go.temporal.io/server/common/resolver"
)

const (
	// PluginName is the name of the plugin
	PluginName = "dsql"
)

// DSQLAction represents the type of DSQL authentication action
type DSQLAction string

const (
	// ActionDbConnectAdmin login as user "admin"
	ActionDbConnectAdmin DSQLAction = "DbConnectAdmin"
	// ActionDbConnect login as a custom DB role/user
	ActionDbConnect DSQLAction = "DbConnect"
)

var defaultDatabaseNames = []string{
	"postgres",  // normal PostgreSQL default DB name
	"defaultdb", // special behavior for Aiven: #1389
}

type plugin struct {
	driver         driver.Driver
	queryConverter sqlplugin.VisibilityQueryConverter
	retryConfig    RetryConfig
}

var _ sqlplugin.Plugin = (*plugin)(nil)

func init() {
	sql.RegisterPlugin(PluginName, &plugin{
		driver:         &driver.PGXDriver{},
		queryConverter: &queryConverter{},
		retryConfig:    DefaultRetryConfig(),
	})
}

func (p *plugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return p.queryConverter
}

// CreateDB initialize the db object
func (p *plugin) CreateDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (sqlplugin.GenericDB, error) {
	connect := func() (*sqlx.DB, error) {
		if cfg.Connect != nil {
			return cfg.Connect(cfg)
		}
		return p.createDSQLConnection(cfg, r, logger)
	}
	needsRefresh := p.driver.IsConnNeedsRefreshError
	handle := sqlplugin.NewDatabaseHandle(dbKind, connect, needsRefresh, logger, metricsHandler, clock.NewRealTimeSource())
	db := newDBWithDependencies(dbKind, cfg.DatabaseName, p.driver, handle, nil, logger, metricsHandler, p.retryConfig)
	return db, nil
}

// createDSQLConnection creates a DSQL database connection with IAM authentication
func (p *plugin) createDSQLConnection(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	logger log.Logger,
) (*sqlx.DB, error) {
	if cfg.DatabaseName != "" {
		return p.createDSQLConnectionWithAuth(cfg, resolver, logger)
	}

	// database name not provided, try defaults
	defer func() { cfg.DatabaseName = "" }()

	var errors []error
	for _, databaseName := range defaultDatabaseNames {
		cfg.DatabaseName = databaseName
		if db, err := p.createDSQLConnectionWithAuth(cfg, resolver, logger); err == nil {
			return db, nil
		} else {
			errors = append(errors, err)
		}
	}
	return nil, serviceerror.NewUnavailable(
		fmt.Sprintf("unable to connect to DSQL, tried default DB names: %v, errors: %v", strings.Join(defaultDatabaseNames, ","), errors),
	)
}

// createDSQLConnectionWithAuth creates a DSQL connection with IAM authentication
func (p *plugin) createDSQLConnectionWithAuth(
	cfg *config.SQL,
	resolver resolver.ServiceResolver,
	logger log.Logger,
) (*sqlx.DB, error) {
	// Get region from environment variable (following AWS DSQL sample pattern)
	region := os.Getenv("REGION")
	if region == "" {
		// Fallback to AWS_REGION if REGION is not set
		region = os.Getenv("AWS_REGION")
		if region == "" {
			return nil, fmt.Errorf("REGION or AWS_REGION environment variable must be set for DSQL IAM authentication")
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

	// Generate DSQL auth token
	authToken, err := p.generateDbConnectAuthToken(context.Background(), region, clusterEndpoint, ActionDbConnectAdmin, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to generate DSQL auth token: %w", err)
	}

	// Create modified config with DSQL auth token
	dsqlConfig := *cfg
	dsqlConfig.User = "admin" // DSQL admin user
	dsqlConfig.Password = authToken

	logger.Info("Creating DSQL connection with IAM authentication",
		tag.NewStringTag("endpoint", clusterEndpoint),
		tag.NewStringTag("region", region),
		tag.NewStringTag("user", dsqlConfig.User),
		tag.NewStringTag("token_expiry", "5m"))

	// Use session to create connection (similar to PostgreSQL plugin)
	dsqlSession, err := session.NewSession(&dsqlConfig, p.driver, resolver)
	if err != nil {
		return nil, err
	}

	logger.Info("DSQL connection established successfully (IAM token refreshed)",
		tag.NewStringTag("endpoint", clusterEndpoint))

	return dsqlSession.DB, nil
}

// generateDbConnectAuthToken generates an IAM auth token for DSQL using AWS SDK
func (p *plugin) generateDbConnectAuthToken(ctx context.Context, region, clusterEndpoint string, action DSQLAction, logger log.Logger) (string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return "", fmt.Errorf("load aws config: %w", err)
	}

	// Token expiry - 1 hour reduces connection refresh frequency
	// Default SDK value is 15 minutes, max is 1 week (604,800 seconds)
	expiry := 1 * time.Hour

	tokenOptions := func(options *auth.TokenOptions) {
		options.ExpiresIn = expiry
	}

	var token string
	if action == ActionDbConnectAdmin {
		// Use admin auth token for admin user
		token, err = auth.GenerateDBConnectAdminAuthToken(ctx, clusterEndpoint, region, cfg.Credentials, tokenOptions)
		if err != nil {
			return "", fmt.Errorf("failed to generate admin auth token: %w", err)
		}
	} else {
		// Use regular auth token for custom users
		token, err = auth.GenerateDbConnectAuthToken(ctx, clusterEndpoint, region, cfg.Credentials, tokenOptions)
		if err != nil {
			return "", fmt.Errorf("failed to generate auth token: %w", err)
		}
	}

	logger.Debug("Generated DSQL auth token via AWS SDK",
		tag.NewStringTag("hostname", clusterEndpoint),
		tag.NewStringTag("region", region),
		tag.NewStringTag("action", string(action)),
		tag.NewStringTag("token_length", fmt.Sprintf("%d", len(token))))

	return token, nil
}
