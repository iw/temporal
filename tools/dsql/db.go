package dsql

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/tools/common/schema"
)

const (
	dsqlDBType = "dsql"

	defaultTokenTTL = 15 * time.Minute

	// Versioning tables (compatible with Temporal schema tooling expectations)
	// These are intentionally simple and DSQL-safe (no SERIAL/BIGSERIAL).
	schemaVersionTableDDL = `
CREATE TABLE IF NOT EXISTS schema_version (
  curr_version            VARCHAR(255) NOT NULL,
  min_compatible_version  VARCHAR(255) NOT NULL,
  last_updated_time       TIMESTAMP NOT NULL,
  PRIMARY KEY (curr_version)
)`

	schemaUpdateHistoryDDL = `
CREATE TABLE IF NOT EXISTS schema_update_history (
  id                 BIGINT NOT NULL,
  old_version        VARCHAR(255) NOT NULL,
  new_version        VARCHAR(255) NOT NULL,
  manifest_md5       VARCHAR(255) NOT NULL,
  description        VARCHAR(1024) NOT NULL,
  applied_at         TIMESTAMP NOT NULL,
  PRIMARY KEY (id)
)`
)

type DSQLSchemaDB struct {
	db     *sql.DB
	logger log.Logger
}

func NewDSQLSchemaDBFromCLI(c *cli.Context, logger log.Logger) (*DSQLSchemaDB, error) {
	ctx := context.Background()

	endpoint := strings.TrimSpace(c.GlobalString(schema.CLIOptEndpoint))
	if endpoint == "" {
		// handler.go uses CLIOptEndpoint; CLIFlagEndpoint maps to "endpoint, ep"
		// In case urfave/cli didn't populate via alias, try the flag name form:
		endpoint = strings.TrimSpace(c.GlobalString("endpoint"))
	}
	if endpoint == "" {
		return nil, fmt.Errorf("missing endpoint; set --%s or CLUSTER_ENDPOINT", schema.CLIOptEndpoint)
	}

	port := c.GlobalInt(schema.CLIOptPort)
	if port == 0 {
		port = 5432
	}

	user := strings.TrimSpace(c.GlobalString(schema.CLIOptUser))
	if user == "" {
		user = "admin"
	}

	dbName := strings.TrimSpace(c.GlobalString(schema.CLIOptDatabase))
	if dbName == "" {
		dbName = "postgres"
	}

	region := strings.TrimSpace(c.GlobalString("region"))
	if region == "" {
		region = os.Getenv("REGION")
	}
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		return nil, fmt.Errorf("AWS region must be provided via --region, REGION, or AWS_REGION")
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	// Token generation: admin vs custom user
	var token string
	if user == "admin" {
		token, err = auth.GenerateDBConnectAdminAuthToken(
			ctx,
			endpoint,
			region,
			awsCfg.Credentials,
			func(o *auth.TokenOptions) { o.ExpiresIn = defaultTokenTTL },
		)
	} else {
		// If/when you support custom DSQL roles
		token, err = auth.GenerateDbConnectAuthToken(
			ctx,
			endpoint,
			region,
			awsCfg.Credentials,
			func(o *auth.TokenOptions) { o.ExpiresIn = defaultTokenTTL },
		)
	}
	if err != nil {
		return nil, fmt.Errorf("generate dsql auth token: %w", err)
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=require",
		url.PathEscape(user),
		url.PathEscape(token),
		endpoint,
		port,
		url.PathEscape(dbName),
	)

	logger.Info("Connecting to DSQL for schema operations",
		tag.NewStringTag("endpoint", endpoint),
		tag.NewStringTag("region", region),
		tag.NewStringTag("database", dbName),
		tag.NewStringTag("user", user),
	)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &DSQLSchemaDB{db: db, logger: logger}, nil
}

func (d *DSQLSchemaDB) Type() string { return dsqlDBType }

func (d *DSQLSchemaDB) Close() { _ = d.db.Close() }

// Exec executes a SQL statement. For DSQL safety, we execute each statement in its own tx.
func (d *DSQLSchemaDB) Exec(stmt string, args ...interface{}) error {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return nil
	}

	ctx := context.Background()

	// one statement per transaction
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// DropAllTables drops all tables in public schema (DSQL-compatible, no DO blocks).
func (d *DSQLSchemaDB) DropAllTables() error {
	ctx := context.Background()

	// List tables in public schema
	rows, err := d.db.QueryContext(ctx, `
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
ORDER BY table_name`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return err
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, t := range tables {
		// Drop each table in its own tx
		stmt := fmt.Sprintf(`DROP TABLE IF EXISTS public.%s`, quoteIdent(t))
		if err := d.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func quoteIdent(s string) string {
	// minimal identifier quoting
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// CreateSchemaVersionTables sets up the schema version tables
func (d *DSQLSchemaDB) CreateSchemaVersionTables() error {
	if err := d.Exec(schemaVersionTableDDL); err != nil {
		return err
	}
	return d.Exec(schemaUpdateHistoryDDL)
}

// ReadSchemaVersion returns the current schema version
func (d *DSQLSchemaDB) ReadSchemaVersion() (string, error) {
	ctx := context.Background()

	// There may be multiple rows if versioning was used differently; we use max by updated time.
	var ver string
	err := d.db.QueryRowContext(ctx, `
SELECT curr_version
FROM schema_version
ORDER BY last_updated_time DESC
LIMIT 1`).Scan(&ver)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return ver, err
}

// UpdateSchemaVersion updates the schema version
func (d *DSQLSchemaDB) UpdateSchemaVersion(newVersion string, minCompatibleVersion string) error {
	ctx := context.Background()

	// Insert a new row to represent the latest version (simple, avoids UPDATE races).
	stmt := `
INSERT INTO schema_version (curr_version, min_compatible_version, last_updated_time)
VALUES ($1, $2, NOW())`
	// one statement per tx
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stmt, newVersion, minCompatibleVersion); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// WriteSchemaUpdateLog adds an entry to schema update history table
func (d *DSQLSchemaDB) WriteSchemaUpdateLog(oldVersion string, newVersion string, manifestMD5 string, desc string) error {
	ctx := context.Background()

	// Generate a stable-ish id: hash of (old,new,md5,desc,timestamp). Keep simple and monotonic-ish.
	h := md5.New()
	_, _ = io.WriteString(h, oldVersion)
	_, _ = io.WriteString(h, newVersion)
	_, _ = io.WriteString(h, manifestMD5)
	_, _ = io.WriteString(h, desc)
	_, _ = io.WriteString(h, time.Now().UTC().Format(time.RFC3339Nano))
	idHex := hex.EncodeToString(h.Sum(nil))
	// take first 16 hex chars as bigint-ish (fits into 64-bit)
	var id int64
	_, _ = fmt.Sscanf(idHex[:16], "%x", &id)
	if id < 0 {
		id = -id
	}

	stmt := `
INSERT INTO schema_update_history
(id, old_version, new_version, manifest_md5, description, applied_at)
VALUES ($1, $2, $3, $4, $5, NOW())`

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stmt, id, oldVersion, newVersion, manifestMD5, desc); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
