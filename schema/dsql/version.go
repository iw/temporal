package dsql

// NOTE: whenever there is a new database schema update, plz update the following versions

// Version is the DSQL persistence database release version
// v1.1: Added current_chasm_executions table for CHASM support (derived from PostgreSQL v1.19)
const Version = "1.1"

// VisibilityVersion is the DSQL visibility database release version (unused; visibility stays on existing store)
const VisibilityVersion = "1.11"
