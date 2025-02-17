package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	BATCH_SIZE                    = 10000
	PING_INTERVAL_BETWEEN_BATCHES = 20
)

type Syncer struct {
	config        *Config
	icebergWriter *IcebergWriter
	icebergReader *IcebergReader
}

type TelemetryData struct {
	DbHost     string `json:"dbHost"`
	OsName     string `json:"osName"`
	DbConnHash string `json:"dbConnHash"`
}

type SyncOptions struct {
	Since time.Time
}

type TableMetadata struct {
	LastSyncTime time.Time `json:"lastSyncTime"`
	RowCount     int64     `json:"rowCount"`
	Checksum     string    `json:"checksum"`
}

func NewSyncer(config *Config) *Syncer {
	if config.Pg.DatabaseUrl == "" {
		panic("Missing PostgreSQL database URL")
	}

	icebergWriter := NewIcebergWriter(config)
	icebergReader := NewIcebergReader(config)
	return &Syncer{config: config, icebergWriter: icebergWriter, icebergReader: icebergReader}
}

func (syncer *Syncer) SyncFromPostgres(options *SyncOptions) {
	ctx := context.Background()
	databaseUrl := syncer.urlEncodePassword(syncer.config.Pg.DatabaseUrl)
	syncer.sendTelemetry(databaseUrl)

	conn, err := pgx.Connect(ctx, databaseUrl)
	PanicIfError(err)
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
	PanicIfError(err)

	pgSchemaTables := []PgSchemaTable{}
	for _, schema := range syncer.listPgSchemas(conn) {
		for _, pgSchemaTable := range syncer.listPgSchemaTables(conn, schema) {
			if syncer.shouldSyncTable(pgSchemaTable) {
				pgSchemaTables = append(pgSchemaTables, pgSchemaTable)
				syncer.syncFromPgTable(conn, pgSchemaTable, options)
			}
		}
	}

	if syncer.config.Pg.SchemaPrefix == "" {
		syncer.deleteOldIcebergSchemaTables(pgSchemaTables)
	}
}

// Example:
// - From postgres://username:pas$:wor^d@host:port/database
// - To postgres://username:pas%24%3Awor%5Ed@host:port/database
func (syncer *Syncer) urlEncodePassword(databaseUrl string) string {
	// No credentials
	if !strings.Contains(databaseUrl, "@") {
		return databaseUrl
	}

	password := strings.TrimPrefix(databaseUrl, "postgresql://")
	password = strings.TrimPrefix(password, "postgres://")
	passwordEndIndex := strings.LastIndex(password, "@")
	password = password[:passwordEndIndex]

	// Credentials without password
	if !strings.Contains(password, ":") {
		return databaseUrl
	}

	_, password, _ = strings.Cut(password, ":")
	decodedPassword, err := url.QueryUnescape(password)
	if err != nil {
		return databaseUrl
	}

	// Password is already encoded
	if decodedPassword != password {
		return databaseUrl
	}

	return strings.Replace(databaseUrl, ":"+password+"@", ":"+url.QueryEscape(password)+"@", 1)
}

func (syncer *Syncer) shouldSyncTable(schemaTable PgSchemaTable) bool {
	tableId := fmt.Sprintf("%s.%s", schemaTable.Schema, schemaTable.Table)

	if syncer.config.Pg.IncludeSchemas != nil {
		if !syncer.config.Pg.IncludeSchemas.Contains(schemaTable.Schema) {
			return false
		}
	} else if syncer.config.Pg.ExcludeSchemas != nil {
		if syncer.config.Pg.ExcludeSchemas.Contains(schemaTable.Schema) {
			return false
		}
	}

	if syncer.config.Pg.IncludeTables != nil {
		return syncer.config.Pg.IncludeTables.Contains(tableId)
	}

	if syncer.config.Pg.ExcludeTables != nil {
		return !syncer.config.Pg.ExcludeTables.Contains(tableId)
	}

	return true
}

func (syncer *Syncer) listPgSchemas(conn *pgx.Conn) []string {
	var schemas []string

	schemasRows, err := conn.Query(
		context.Background(),
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'pg_toast', 'information_schema')",
	)
	PanicIfError(err)
	defer schemasRows.Close()

	for schemasRows.Next() {
		var schema string
		err = schemasRows.Scan(&schema)
		PanicIfError(err)
		schemas = append(schemas, schema)
	}

	return schemas
}

func (syncer *Syncer) listPgSchemaTables(conn *pgx.Conn, schema string) []PgSchemaTable {
	var pgSchemaTables []PgSchemaTable

	tablesRows, err := conn.Query(
		context.Background(),
		`
		SELECT pg_class.relname AS table, COALESCE(parent.relname, '') AS parent_partitioned_table
		FROM pg_class
		JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
		LEFT JOIN pg_inherits ON pg_inherits.inhrelid = pg_class.oid
		LEFT JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
		WHERE pg_namespace.nspname = $1 AND pg_class.relkind = 'r';
		`,
		schema,
	)
	PanicIfError(err)
	defer tablesRows.Close()

	for tablesRows.Next() {
		pgSchemaTable := PgSchemaTable{Schema: schema}
		err = tablesRows.Scan(&pgSchemaTable.Table, &pgSchemaTable.ParentPartitionedTable)
		PanicIfError(err)
		pgSchemaTables = append(pgSchemaTables, pgSchemaTable)
	}

	return pgSchemaTables
}

func (syncer *Syncer) syncFromPgTable(conn *pgx.Conn, pgSchemaTable PgSchemaTable, options *SyncOptions) {
	LogInfo(syncer.config, "Syncing "+pgSchemaTable.String()+"...")

	// Get table metadata for incremental sync
	metadata, err := syncer.getTableMetadata(pgSchemaTable)
	PanicIfError(err)

	// If incremental sync is requested and table hasn't changed, skip it
	if options != nil && !options.Since.IsZero() {
		if metadata.LastSyncTime.After(options.Since) && !syncer.hasTableChanged(conn, pgSchemaTable, metadata) {
			LogInfo(syncer.config, "Skipping "+pgSchemaTable.String()+" - no changes since last sync")
			return
		}
	}

	csvFile, err := syncer.exportPgTableToCsv(conn, pgSchemaTable)
	PanicIfError(err)
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	csvHeader, err := csvReader.Read()
	PanicIfError(err)

	pgSchemaColumns := syncer.pgTableSchemaColumns(conn, pgSchemaTable, csvHeader)
	reachedEnd := false
	totalRowCount := 0

	schemaTable := pgSchemaTable.ToIcebergSchemaTable()
	syncer.icebergWriter.Write(schemaTable, pgSchemaColumns, func() [][]string {
		if reachedEnd {
			return [][]string{}
		}

		var rows [][]string
		for {
			row, err := csvReader.Read()
			if err != nil {
				reachedEnd = true
				break
			}

			rows = append(rows, row)
			if len(rows) >= BATCH_SIZE {
				break
			}
		}

		totalRowCount += len(rows)
		LogDebug(syncer.config, "Writing", totalRowCount, "rows to Parquet...")

		// Ping the database to prevent the connection from being closed
		if totalRowCount%(BATCH_SIZE*PING_INTERVAL_BETWEEN_BATCHES) == 0 {
			LogDebug(syncer.config, "Pinging the database...")
			_, err := conn.Exec(context.Background(), "SELECT 1")
			PanicIfError(err)
		}

		return rows
	})

	// Update table metadata after successful sync
	metadata.LastSyncTime = time.Now()
	metadata.RowCount = int64(totalRowCount)
	metadata.Checksum = syncer.calculateTableChecksum(conn, pgSchemaTable)
	err = syncer.saveTableMetadata(pgSchemaTable, metadata)
	PanicIfError(err)
}

func (syncer *Syncer) pgTableSchemaColumns(conn *pgx.Conn, pgSchemaTable PgSchemaTable, csvHeader []string) []PgSchemaColumn {
	var pgSchemaColumns []PgSchemaColumn

	rows, err := conn.Query(
		context.Background(),
		`SELECT
			column_name,
			data_type,
			udt_name,
			is_nullable,
			ordinal_position,
			COALESCE(character_maximum_length, 0),
			COALESCE(numeric_precision, 0),
			COALESCE(numeric_scale, 0),
			COALESCE(datetime_precision, 0),
			pg_namespace.nspname
		FROM information_schema.columns
		JOIN pg_type ON pg_type.typname = udt_name
		JOIN pg_namespace ON pg_namespace.oid = pg_type.typnamespace
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY array_position($3, column_name)`,
		pgSchemaTable.Schema,
		pgSchemaTable.Table,
		csvHeader,
	)
	PanicIfError(err)
	defer rows.Close()

	for rows.Next() {
		var pgSchemaColumn PgSchemaColumn
		err = rows.Scan(
			&pgSchemaColumn.ColumnName,
			&pgSchemaColumn.DataType,
			&pgSchemaColumn.UdtName,
			&pgSchemaColumn.IsNullable,
			&pgSchemaColumn.OrdinalPosition,
			&pgSchemaColumn.CharacterMaximumLength,
			&pgSchemaColumn.NumericPrecision,
			&pgSchemaColumn.NumericScale,
			&pgSchemaColumn.DatetimePrecision,
			&pgSchemaColumn.Namespace,
		)
		PanicIfError(err)
		pgSchemaColumns = append(pgSchemaColumns, pgSchemaColumn)
	}

	return pgSchemaColumns
}

func (syncer *Syncer) exportPgTableToCsv(conn *pgx.Conn, pgSchemaTable PgSchemaTable) (csvFile *os.File, err error) {
	tempFile, err := CreateTemporaryFile(pgSchemaTable.String())
	PanicIfError(err)
	defer DeleteTemporaryFile(tempFile)

	result, err := conn.PgConn().CopyTo(
		context.Background(),
		tempFile,
		"COPY "+pgSchemaTable.String()+" TO STDOUT WITH CSV HEADER NULL '"+PG_NULL_STRING+"'",
	)
	PanicIfError(err)
	LogDebug(syncer.config, "Copied", result.RowsAffected(), "row(s) into", tempFile.Name())

	return os.Open(tempFile.Name())
}

func (syncer *Syncer) deleteOldIcebergSchemaTables(pgSchemaTables []PgSchemaTable) {
	var prefixedPgSchemaTables []PgSchemaTable
	for _, pgSchemaTable := range pgSchemaTables {
		prefixedPgSchemaTables = append(
			prefixedPgSchemaTables,
			PgSchemaTable{Schema: syncer.config.Pg.SchemaPrefix + pgSchemaTable.Schema, Table: pgSchemaTable.Table},
		)
	}

	icebergSchemas, err := syncer.icebergReader.Schemas()
	PanicIfError(err)

	for _, icebergSchema := range icebergSchemas {
		found := false
		for _, pgSchemaTable := range prefixedPgSchemaTables {
			if icebergSchema == pgSchemaTable.Schema {
				found = true
				break
			}
		}

		if !found {
			LogInfo(syncer.config, "Deleting", icebergSchema, "...")
			syncer.icebergWriter.DeleteSchema(icebergSchema)
		}
	}

	icebergSchemaTables, err := syncer.icebergReader.SchemaTables()
	PanicIfError(err)

	for _, icebergSchemaTable := range icebergSchemaTables.Values() {
		found := false
		for _, pgSchemaTable := range prefixedPgSchemaTables {
			if icebergSchemaTable.String() == pgSchemaTable.String() {
				found = true
				break
			}
		}

		if !found {
			LogInfo(syncer.config, "Deleting", icebergSchemaTable.String(), "...")
			syncer.icebergWriter.DeleteSchemaTable(icebergSchemaTable)
		}
	}
}

func (syncer *Syncer) isLocalHost(hostname string) bool {
	switch hostname {
	case "localhost", "127.0.0.1", "::1", "0.0.0.0":
		return true
	}
	return false
}

func (syncer *Syncer) sendTelemetry(databaseUrl string) {
	if syncer.config.DisableAnalytics {
		LogInfo(syncer.config, "Telemetry is disabled")
		return
	}

	dbUrl, err := url.Parse(databaseUrl)
	if err != nil {
		return
	}

	hostname := dbUrl.Hostname()
	if syncer.isLocalHost(hostname) {
		return
	}

	data := TelemetryData{
		DbHost:     hostname,
		OsName:     runtime.GOOS,
		DbConnHash: StringToSha256Hash(databaseUrl),
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}

	client := http.Client{Timeout: 5 * time.Second}
	_, _ = client.Post("http://api.bemidb.com/api/analytics", "application/json", bytes.NewBuffer(jsonData))
}

func (syncer *Syncer) getTableMetadata(pgSchemaTable PgSchemaTable) (TableMetadata, error) {
	metadataPath := filepath.Join(syncer.config.StoragePath, "metadata", pgSchemaTable.Schema, pgSchemaTable.Table+".json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return TableMetadata{}, nil
		}
		return TableMetadata{}, err
	}

	var metadata TableMetadata
	err = json.Unmarshal(data, &metadata)
	return metadata, err
}

func (syncer *Syncer) saveTableMetadata(pgSchemaTable PgSchemaTable, metadata TableMetadata) error {
	metadataDir := filepath.Join(syncer.config.StoragePath, "metadata", pgSchemaTable.Schema)
	err := os.MkdirAll(metadataDir, 0755)
	if err != nil {
		return err
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	metadataPath := filepath.Join(metadataDir, pgSchemaTable.Table+".json")
	return os.WriteFile(metadataPath, data, 0644)
}

func (syncer *Syncer) hasTableChanged(conn *pgx.Conn, pgSchemaTable PgSchemaTable, metadata TableMetadata) bool {
	currentChecksum := syncer.calculateTableChecksum(conn, pgSchemaTable)
	return currentChecksum != metadata.Checksum
}

func (syncer *Syncer) calculateTableChecksum(conn *pgx.Conn, pgSchemaTable PgSchemaTable) string {
	query := fmt.Sprintf("SELECT COUNT(*), SUM(hashtext(CAST(t.* AS text))) FROM %s.%s t",
		pgSchemaTable.Schema, pgSchemaTable.Table)
	
	var count int64
	var checksum string
	err := conn.QueryRow(context.Background(), query).Scan(&count, &checksum)
	if err != nil {
		return ""
	}
	
	return fmt.Sprintf("%d:%s", count, checksum)
}


