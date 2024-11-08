package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/chamot1111/replayforge/pkgs/playerplugin"

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
}

func (s *SqliteSink) Init(config playerplugin.SinkConfig) error {
	// Initialize the SqliteSink with the provided configuration
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
		listenHost = "localhost" // Default to localhost if not specified
	}

	s.ListenAddr = fmt.Sprintf("%s:%s", listenHost, listenPort)

	s.StaticDir, ok = params["static_dir"].(string)
	if !ok {
		s.StaticDir = "" // Make static_dir optional by using empty string as default
	}

	db, err := sql.Open("sqlite3", s.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	s.DB = db
	s.ID = config.ID
	s.TableSchemas = make(map[string]map[string]string)
	s.loadTableSchemas()

	// Add _rpf_last_updated_idx column to all tables and create index
	for table := range s.TableSchemas {
		err = s.ensureColumnExists(table, "_rpf_last_updated_idx", "INTEGER")
		if err != nil {
			return fmt.Errorf("failed to add _rpf_last_updated_idx column: %v", err)
		}
	}

	// Start TTL cleanup goroutine if TTL is set
	if s.RowTTLSec > 0 {
		go s.ttlCleanup()
	}

	return nil
}

func (s *SqliteSink) Start() error {
	// Start HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("/%s/rpf-db/", s.ID), s.handleGetRequest)
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
		// Open a new DB connection for this goroutine
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

func (s *SqliteSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels map[string]chan string) error {
	table := strings.TrimPrefix(path, "/rpf-db/")
	table = strings.TrimPrefix(table, "/")

	// Ensure table name is compatible with SQL by allowing only alphanumeric characters, underscore and dash
	if !isValidTableName(table) {
		return fmt.Errorf("invalid table name: %s - only alphanumeric characters, underscore and dash are allowed", table)
	}

	err := s.ensureTableExists(table)
	if err != nil {
		return err
	}

	switch method {
	case "POST":
		return s.handlePost(table, body)
	case "PUT":
		return s.handlePut(table, body)
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

	}
	return nil
}

func (s *SqliteSink) ensureColumnExists(table, column, dataType string) error {
	if _, exists := s.TableSchemas[table][column]; !exists {
		query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, dataType)
		_, err := s.DB.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to add column %s to table %s: %v", column, table, err)
		}
		s.TableSchemas[table][column] = dataType

		// Create index if column name ends with idx, index or id
		if strings.HasSuffix(column, "idx") || strings.HasSuffix(column, "index") || strings.HasSuffix(column, "id") {
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

	if _, ok := data["id"]; !ok {
		return fmt.Errorf("id is mandatory")
	}

	data["_rpf_last_updated_idx"] = time.Now().Unix()

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

func (s *SqliteSink) handlePut(table string, body []byte) error {
	var data map[string]interface{}
	err := json.Unmarshal(body, &data)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v", err)
	}
	id, ok := data["id"]
	if !ok {
		return fmt.Errorf("id is required for update")
	}

	data["_rpf_last_updated_idx"] = time.Now().Unix()

	delete(data, "id")
	setStatements := make([]string, 0, len(data))
	values := make([]interface{}, 0, len(data)+1)
	for k, v := range data {
		setStatements = append(setStatements, fmt.Sprintf("%s = ?", k))
		values = append(values, v)
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
	values = append(values, id)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?",
		table,
		strings.Join(setStatements, ", "))
	_, err = s.DB.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to update: %v", err)
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

func (s *SqliteSink) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	table := strings.TrimPrefix(r.URL.Path, fmt.Sprintf("/%s/rpf-db/", s.ID))
	query := fmt.Sprintf("SELECT * FROM %s", table)

	// Parse query parameters
	whereClause := ""
	orderByClause := ""
	limitClause := ""

	for key, values := range r.URL.Query() {
		if key == "order" {
			orderByClause = fmt.Sprintf(" ORDER BY %s", values[0])
		} else if key == "limit" {
			limitClause = fmt.Sprintf(" LIMIT %s", values[0])
		} else {
			if whereClause == "" {
				whereClause = " WHERE "
			} else {
				whereClause += " AND "
			}
			whereClause += fmt.Sprintf("%s = ?", key)
			query += whereClause
		}
	}

	query += orderByClause + limitClause

	// Prepare and execute the query
	stmt, err := s.DB.Prepare(query)
	if err != nil {
		logger.Error("Error preparing query: %v", err)
		http.Error(w, fmt.Sprintf("Error preparing query: %v", err), http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	// Extract values for WHERE clause
	var queryArgs []interface{}
	for key, values := range r.URL.Query() {
		if key != "order" && key != "limit" {
			queryArgs = append(queryArgs, values[0])
		}
	}

	rows, err := stmt.Query(queryArgs...)
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
	json.NewEncoder(w).Encode(result)
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
