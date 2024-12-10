package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/chamot1111/replayforge/pkgs/playerplugin"

	"runtime"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type SqliteSink struct {
	DB           *sql.DB
	TableSchemas map[string]map[string]string
	ListenAddr   string
	StaticDir    string
	ID           string
	ExposedPort  int
	RowTTLSec    int64
	DBPath       string
	LastRpfID    int64
}

func (s *SqliteSink) Init(config playerplugin.SinkConfig, sinkChannels *sync.Map) error {
	var params map[string]interface{}
	err := json.Unmarshal(config.Params, &params)
	if err != nil {
		return fmt.Errorf("failed to parse params: %v", err)
	}

	if ttl, ok := params["row_ttl_sec"].(float64); ok {
		s.RowTTLSec = int64(ttl)
	}

	dbPath, ok := params["database"].(string)
	if !ok {
		return fmt.Errorf("database path not found in params or not a string")
	}

	dbPathWithQuery := dbPath + "?_auto_vacuum=1&_journal_mode=WAL&_synchronous=NORMAL"
	s.DBPath = dbPathWithQuery

	listenPort, ok := params["listen_port"].(string)
	if !ok {
		return fmt.Errorf("listen_port not found in params or not a string")
	} else {
		port, err := strconv.Atoi(listenPort)
		if err != nil {
			return fmt.Errorf("failed to parse listen_port as integer: %v", err)
		}
		s.ExposedPort = port
	}

	listenHost, ok := params["listen_host"].(string)
	if !ok {
		listenHost = "localhost"
	}

	s.ListenAddr = fmt.Sprintf("%s:%s", listenHost, listenPort)

	s.StaticDir, ok = params["static_dir"].(string)
	if !ok {
		s.StaticDir = ""
	}

	db, err := sql.Open("sqlite3", s.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	s.DB = db
	s.ID = config.ID
	s.TableSchemas = make(map[string]map[string]string)
	s.loadTableSchemas()
	s.LastRpfID = 0

	for table := range s.TableSchemas {
		err = s.ensureColumnExists(table, "_rpf_last_updated_idx", "INTEGER")
		if err != nil {
			return fmt.Errorf("failed to add _rpf_last_updated_idx column: %v", err)
		}
		err = s.ensureColumnExists(table, "_rpf_id", "INTEGER")
		if err != nil {
			return fmt.Errorf("failed to add _rpf_id column: %v", err)
		}

		// Get max _rpf_id for table
		var maxID sql.NullInt64
		row := s.DB.QueryRow(fmt.Sprintf("SELECT MAX(_rpf_id) FROM %s", table))
		err = row.Scan(&maxID)
		if err != nil {
			return fmt.Errorf("failed to get max _rpf_id: %v", err)
		}
		if maxID.Valid && maxID.Int64 > s.LastRpfID {
			s.LastRpfID = maxID.Int64
		}
	}

	if s.RowTTLSec > 0 {
		go s.ttlCleanup()
	}

	return nil
}

func (s *SqliteSink) handleListTables(w http.ResponseWriter, r *http.Request) {
	query := `SELECT name FROM sqlite_schema WHERE type='table'`
	rows, err := s.DB.Query(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error listing tables: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning table name: %v", err), http.StatusInternalServerError)
			return
		}
		tables = append(tables, tableName)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(tables)
}

func (s *SqliteSink) handleListColumns(w http.ResponseWriter, r *http.Request) {
	tableName := strings.TrimPrefix(r.URL.Path, fmt.Sprintf("/%s/columns/", s.ID))
	if tableName == "" {
		http.Error(w, "Table name is required", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := s.DB.Query(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting table columns: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var dfltValue interface{}
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning column info: %v", err), http.StatusInternalServerError)
			return
		}
		columns = append(columns, name)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(columns)
}

func (s *SqliteSink) handleDisinctValuesForTableColumns(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 3 {
		http.Error(w, "Invalid path format", http.StatusBadRequest)
		return
	}

	table := pathParts[len(pathParts)-2]
	column := pathParts[len(pathParts)-1]

	query := fmt.Sprintf("SELECT DISTINCT %s FROM %s", column, table)

	rows, err := s.DB.Query(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying distinct values: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var values []interface{}
	for rows.Next() {
		var value interface{}
		if err := rows.Scan(&value); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning value: %v", err), http.StatusInternalServerError)
			return
		}

		// Convert []byte to string if necessary
		if b, ok := value.([]byte); ok {
			value = string(b)
		}

		values = append(values, value)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(values)
}

func (s *SqliteSink) handleListAllTablesColumns(w http.ResponseWriter, r *http.Request) {
	tableRows, err := s.DB.Query("SELECT name FROM sqlite_schema WHERE type='table'")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error listing tables: %v", err), http.StatusInternalServerError)
		return
	}
	defer tableRows.Close()

	result := make(map[string][]string)

	for tableRows.Next() {
		var tableName string
		if err := tableRows.Scan(&tableName); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning table name: %v", err), http.StatusInternalServerError)
			return
		}

		columnRows, err := s.DB.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting columns for table %s: %v", tableName, err), http.StatusInternalServerError)
			return
		}
		defer columnRows.Close()

		var columns []string
		for columnRows.Next() {
			var cid int
			var name, dataType string
			var notNull, pk int
			var dfltValue interface{}
			if err := columnRows.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
				http.Error(w, fmt.Sprintf("Error scanning column info: %v", err), http.StatusInternalServerError)
				return
			}
			columns = append(columns, name)
		}

		result[tableName] = columns
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *SqliteSink) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("/%s/rpf-db/", s.ID), s.handleGetRequest)
	mux.HandleFunc(fmt.Sprintf("/%s/query-table/", s.ID), s.handleGetRequest)
	mux.HandleFunc(fmt.Sprintf("/%s/find/", s.ID), s.handleFindRequest)
	mux.HandleFunc(fmt.Sprintf("/%s/list", s.ID), s.handleListTables)
	mux.HandleFunc(fmt.Sprintf("/%s/columns/", s.ID), s.handleListColumns)
	mux.HandleFunc(fmt.Sprintf("/%s/all-columns/", s.ID), s.handleListAllTablesColumns)
	mux.HandleFunc(fmt.Sprintf("/%s/distinct-values/", s.ID), s.handleDisinctValuesForTableColumns)
	mux.HandleFunc(fmt.Sprintf("/%s/time-analytics/", s.ID), s.handleTimeAnalytics)

	if s.StaticDir != "" {
		mux.Handle(fmt.Sprintf("/%s/static/", s.ID), http.StripPrefix(fmt.Sprintf("/%s/static/", s.ID), http.FileServer(http.Dir(s.StaticDir))))
	}
	logger.Info("Starting HTTP server on %s", s.ListenAddr)
	go func() {
		srv := &http.Server{
			Addr:    s.ListenAddr,
			Handler: mux,
		}
		err := srv.ListenAndServe()
		if err != nil {
			logger.Fatal("Failed to start HTTP server: %v", err)
		}
	}()
	return nil
}

func (s *SqliteSink) ttlCleanup() {
	ticker := time.NewTicker(time.Duration(s.RowTTLSec) * time.Second)
	for range ticker.C {
		db, err := sql.Open("sqlite3", s.DBPath)
		if err != nil {
			logger.Error("Failed to open DB connection for TTL cleanup: %v", err)
			continue
		}

		now := time.Now().Unix()
		for table := range s.TableSchemas {
			query := fmt.Sprintf("DELETE FROM %s WHERE _rpf_last_updated_idx < ?", table)
			expiryTime := now - s.RowTTLSec
			_, err := db.Exec(query, expiryTime)
			if err != nil {
				logger.Error("Failed to cleanup expired rows: %v", err)
			}
		}

		db.Close()
	}
}

func isValidTableName(table string) bool {
	for _, c := range table {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func (s *SqliteSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels *sync.Map) error {
	table := strings.TrimPrefix(path, "/rpf-db/")
	table = strings.TrimPrefix(table, "/")

	if !isValidTableName(table) {
		return fmt.Errorf("invalid table name: %s - only alphanumeric characters, underscore and dash are allowed", table)
	}

	err := s.ensureTableExists(table)
	if err != nil {
		return err
	}

	switch method {
	case "POST", "PUT":
		return s.handlePost(table, body)
	case "DELETE":
		return s.handleDelete(table, body)
	default:
		return fmt.Errorf("unsupported method: %s", method)
	}
}

func (s *SqliteSink) Close() error {
	return s.DB.Close()
}

func (s *SqliteSink) ensureTableExists(table string) error {
	if _, exists := s.TableSchemas[table]; !exists {
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY)", table)
		_, err := s.DB.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %v", table, err)
		}
		s.TableSchemas[table] = make(map[string]string)
		s.TableSchemas[table]["id"] = "TEXT"

		err = s.ensureColumnExists(table, "_rpf_last_updated_idx", "INTEGER")
		if err != nil {
			return fmt.Errorf("failed to add _rpf_last_updated_idx column: %v", err)
		}

		err = s.ensureColumnExists(table, "_rpf_id", "INTEGER")
		if err != nil {
			return fmt.Errorf("failed to add _rpf_id column: %v", err)
		}

	}
	return nil
}

func (s *SqliteSink) ensureColumnExists(table, column, dataType string) error {
	if _, exists := s.TableSchemas[table][column]; !exists {
		query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, dataType)
		_, err := s.DB.Exec(query)
		if err != nil {
			stackTrace := make([]byte, 8192)
			runtime.Stack(stackTrace, false)
			return fmt.Errorf("failed to add column %s to table %s: %v\nStack trace:\n%s", column, table, err, string(stackTrace))
		}
		s.TableSchemas[table][column] = dataType

		if strings.HasSuffix(column, "idx") ||
			strings.HasSuffix(column, "index") ||
			strings.HasSuffix(column, "id") ||
			strings.Contains(column, "fk") ||
			column == "timestamp" ||
			column == "status" {
			indexQuery := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s(%s)",
				table, column, table, column)
			_, err = s.DB.Exec(indexQuery)
			if err != nil {
				return fmt.Errorf("failed to create index on column %s: %v", column, err)
			}
		}
	}
	return nil
}

func (s *SqliteSink) handlePost(table string, body []byte) error {
	var data map[string]interface{}
	err := json.Unmarshal(body, &data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	newData := make(map[string]interface{})
	for k, v := range data {
		newKey := sanitizeIdentifier(k)
		newData[newKey] = v
	}
	data = newData

	data["_rpf_last_updated_idx"] = time.Now().Unix()
	s.LastRpfID++
	data["_rpf_id"] = s.LastRpfID

	if _, ok := data["id"]; !ok {
		data["id"] = strconv.FormatInt(s.LastRpfID, 10)
	}

	timestamp := normalizeTimestamp(data["timestamp"])
	data["timestamp"] = timestamp

	columns := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data))
	placeholders := make([]string, 0, len(data))
	for k, v := range data {
		columns = append(columns, k)
		values = append(values, v)
		placeholders = append(placeholders, "?")
		dataType := "TEXT"
		switch v.(type) {
		case int, int64:
			dataType = "INTEGER"
		case float64:
			dataType = "REAL"
		case bool:
			dataType = "BOOLEAN"
		}
		err = s.ensureColumnExists(table, k, dataType)
		if err != nil {
			return err
		}
	}
	query := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))
	_, err = s.DB.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert or update: %v", err)
	}
	return nil
}

func (s *SqliteSink) handleDelete(table string, body []byte) error {
	var data map[string]interface{}
	err := json.Unmarshal(body, &data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	id, ok := data["id"]
	if !ok {
		return fmt.Errorf("id is required for delete")
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", table)
	_, err = s.DB.Exec(query, id)

	if err != nil {
		return fmt.Errorf("failed to delete: %v", err)
	}
	return nil
}

func (s *SqliteSink) handleFindRequest(w http.ResponseWriter, r *http.Request) {
	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 {
		http.Error(w, "Invalid path format", http.StatusBadRequest)
		return
	}

	table := pathParts[len(pathParts)-2]
	id := pathParts[len(pathParts)-1]

	// First get the column names
	columnRows, err := s.DB.Query(fmt.Sprintf("SELECT * FROM %s WHERE 1=0", table))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting columns: %v", err), http.StatusInternalServerError)
		return
	}
	columns, err := columnRows.Columns()
	columnRows.Close()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting columns: %v", err), http.StatusInternalServerError)
		return
	}

	// Now query the actual row
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = ?", table)
	row := s.DB.QueryRow(query, id)

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	err = row.Scan(valuePtrs...)
	if err == sql.ErrNoRows {
		http.Error(w, "Record not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
		return
	}

	result := make(map[string]interface{})
	for i, col := range columns {
		var v interface{}
		val := values[i]
		b, ok := val.([]byte)
		if ok {
			v = string(b)
		} else {
			v = val
		}
		result[col] = v
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (s *SqliteSink) handleTimeAnalytics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	table := r.URL.Query().Get("table")
	if table == "" {
		http.Error(w, "Table parameter is required", http.StatusBadRequest)
		return
	}

	startTime := r.URL.Query().Get("start_time")
	if startTime == "" {
		http.Error(w, "start_time parameter is required", http.StatusBadRequest)
		return
	}

	stopTime := r.URL.Query().Get("stop_time")
	if stopTime == "" {
		http.Error(w, "stop_time parameter is required", http.StatusBadRequest)
		return
	}

	rangeTime := r.URL.Query().Get("range_time")
	if rangeTime == "" {
		http.Error(w, "range_time parameter is required", http.StatusBadRequest)
		return
	}

	// Convert range_time to seconds
	rangeDuration, err := time.ParseDuration(rangeTime)
	if err != nil {
		http.Error(w, "Invalid range_time format. Use format like '1h', '30m', '1d'", http.StatusBadRequest)
		return
	}
	rangeSeconds := int64(rangeDuration.Seconds())

	// Check if status column exists
	hasStatus := false
	columns, err := s.DB.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying table schema: %v", err), http.StatusInternalServerError)
		return
	}
	defer columns.Close()

	for columns.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var dfltValue interface{}
		if err := columns.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning column info: %v", err), http.StatusInternalServerError)
			return
		}
		if name == "status" {
			hasStatus = true
			break
		}
	}

	// Prepare the query based on whether status column exists
	var query string
	if hasStatus {
		query = fmt.Sprintf(`
												WITH RECURSIVE
												time_ranges AS (
																SELECT CAST(? AS INTEGER) as range_start
																UNION ALL
																SELECT range_start + ?
																FROM time_ranges
																WHERE range_start + ? <= CAST(? AS INTEGER)
												)
												SELECT
																tr.range_start as time_bucket,
																COALESCE(t.status, 'unknown') as status,
																COUNT(t._rpf_id) as count
												FROM time_ranges tr
												LEFT JOIN %s t ON t._rpf_last_updated_idx >= tr.range_start
																AND t._rpf_last_updated_idx < tr.range_start + ?
																AND t._rpf_last_updated_idx >= CAST(? AS INTEGER)
																AND t._rpf_last_updated_idx <= CAST(? AS INTEGER)
												GROUP BY tr.range_start, t.status
												ORDER BY tr.range_start, t.status
								`, table)
	} else {
		query = fmt.Sprintf(`
												WITH RECURSIVE
												time_ranges AS (
																SELECT CAST(? AS INTEGER) as range_start
																UNION ALL
																SELECT range_start + ?
																FROM time_ranges
																WHERE range_start + ? <= CAST(? AS INTEGER)
												)
												SELECT
																tr.range_start as time_bucket,
																COUNT(t._rpf_id) as count
												FROM time_ranges tr
												LEFT JOIN %s t ON t._rpf_last_updated_idx >= tr.range_start
																AND t._rpf_last_updated_idx < tr.range_start + ?
																AND t._rpf_last_updated_idx >= CAST(? AS INTEGER)
																AND t._rpf_last_updated_idx <= CAST(? AS INTEGER)
												GROUP BY tr.range_start
												ORDER BY tr.range_start
								`, table)
	}

	// Execute query
	var rows *sql.Rows
	if hasStatus {
		rows, err = s.DB.Query(query, startTime, rangeSeconds, rangeSeconds, stopTime, rangeSeconds, startTime, stopTime)
	} else {
		rows, err = s.DB.Query(query, startTime, rangeSeconds, rangeSeconds, stopTime, rangeSeconds, startTime, stopTime)
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Process results
	var results []map[string]interface{}
	for rows.Next() {
		result := make(map[string]interface{})
		if hasStatus {
			var timeBucket int64
			var status string
			var count int
			if err := rows.Scan(&timeBucket, &status, &count); err != nil {
				http.Error(w, fmt.Sprintf("Error scanning results: %v", err), http.StatusInternalServerError)
				return
			}
			result["time_bucket"] = timeBucket
			result["status"] = status
			result["count"] = count
		} else {
			var timeBucket int64
			var count int
			if err := rows.Scan(&timeBucket, &count); err != nil {
				http.Error(w, fmt.Sprintf("Error scanning results: %v", err), http.StatusInternalServerError)
				return
			}
			result["time_bucket"] = timeBucket
			result["count"] = count
		}
		results = append(results, result)
	}

	// Return results
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (s *SqliteSink) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	table := strings.TrimPrefix(r.URL.Path, fmt.Sprintf("/%s/rpf-db/", s.ID))
	table = strings.TrimPrefix(table, fmt.Sprintf("/%s/query-table/", s.ID))
	query := fmt.Sprintf("SELECT * FROM %s", table)
	var conditions []string
	var args []interface{}
	orderByClause := " ORDER BY _rpf_id" // Default order
	limitClause := " LIMIT 10000"        // Default limit

	for key, values := range r.URL.Query() {
		if key == "order" {
			parts := strings.Split(values[0], ":")
			if len(parts) == 2 {
				direction := strings.ToUpper(parts[1])
				if direction == "ASC" || direction == "DESC" {
					orderByClause = fmt.Sprintf(" ORDER BY %s %s", parts[0], direction)
				}
			} else {
				orderByClause = fmt.Sprintf(" ORDER BY %s", values[0])
			}
		} else if key == "limit" {
			limitClause = fmt.Sprintf(" LIMIT %s", values[0])
		} else {
			sanitizedKey := sanitizeIdentifier(key)
			if strings.HasSuffix(key, "_contains") {
				conditions = append(conditions, fmt.Sprintf("%s LIKE ?", strings.TrimSuffix(sanitizedKey, "_contains")))
				args = append(args, fmt.Sprintf("%%%s%%", values[0]))
			} else if strings.HasSuffix(key, "_gt") {
				castedValue, castPlaceholder := s.castValue(table, strings.TrimSuffix(sanitizedKey, "_gt"), values[0])
				conditions = append(conditions, fmt.Sprintf("%s > %s", strings.TrimSuffix(sanitizedKey, "_gt"), castPlaceholder))
				args = append(args, castedValue)
			} else if strings.HasSuffix(key, "_lt") {
				castedValue, castPlaceholder := s.castValue(table, strings.TrimSuffix(sanitizedKey, "_lt"), values[0])
				conditions = append(conditions, fmt.Sprintf("%s < %s", strings.TrimSuffix(sanitizedKey, "_lt"), castPlaceholder))
				args = append(args, castedValue)
			} else {
				conditions = append(conditions, fmt.Sprintf("%s = ?", sanitizedKey))
				args = append(args, values[0])
			}
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += orderByClause + limitClause

	stmt, err := s.DB.Prepare(query)
	if err != nil {
		logger.Error("Error preparing query: %v", err)
		http.Error(w, fmt.Sprintf("Error preparing query: %v", err), http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		logger.Error("Error querying table: %v", err)
		http.Error(w, fmt.Sprintf("Error querying table: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		logger.Error("Error get columns: %v", err)
		http.Error(w, fmt.Sprintf("Error get columns: %v", err), http.StatusInternalServerError)
		return
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			logger.Error("Error scanning row: %v", err)
			http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		result = append(result, entry)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-SQL-Query", query)
	json.NewEncoder(w).Encode(result)
}

func (s *SqliteSink) castValue(table, column, value string) (interface{}, string) {
	dataType, exists := s.TableSchemas[table][column]
	if !exists {
		return value, "?"
	}

	switch dataType {
	case "INTEGER":
		if i, err := strconv.Atoi(value); err == nil {
			return i, "?"
		}
	case "REAL":
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f, "?"
		}
	case "DATETIME":
		return value, "DATETIME(?)"
	}

	return value, "?"
}

func sanitizeIdentifier(input string) string {
	// Only allow alphanumeric characters and underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	processed := reg.ReplaceAllString(input, "")
	// Convert camelCase to snake_case
	snakeCase := regexp.MustCompile(`([a-z0-9])([A-Z])`).ReplaceAllString(processed, "${1}_${2}")
	return strings.ToLower(snakeCase)
}

func (s *SqliteSink) loadTableSchemas() {
	rows, err := s.DB.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		logger.Fatal("Failed to query tables: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			logger.Fatal("Failed to scan table name: %v", err)
		}

		tableInfo, err := s.DB.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
		if err != nil {
			logger.Fatal("Failed to get table info for %s: %v", tableName, err)
		}
		defer tableInfo.Close()

		s.TableSchemas[tableName] = make(map[string]string)
		for tableInfo.Next() {
			var cid int
			var name, dataType string
			var notNull, pk int
			var dfltValue interface{}
			if err := tableInfo.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
				logger.Fatal("Failed to scan column info: %v", err)
			}
			s.TableSchemas[tableName][name] = dataType
		}
	}
}

func (s *SqliteSink) GetID() string {
	return s.ID
}

func (s *SqliteSink) GetExposedPort() (int, bool) {
	return s.ExposedPort, true
}
