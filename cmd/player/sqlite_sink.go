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

	_ "github.com/mattn/go-sqlite3"
)

type TimeseriesType string

const (
	GaugeType    TimeseriesType = "gauge"
	CounterType  TimeseriesType = "counter"
	RollupType   TimeseriesType = "rollup"
	DeltaType    TimeseriesType = "delta"
	SnapshotType TimeseriesType = "snapshot"
)

type TimeseriesConfig struct {
	Type      TimeseriesType
	TableName string
	Interval  time.Duration
}

type SqliteSink struct {
	DB               *sql.DB
	TableSchemas     map[string]map[string]string
	TimeseriesTables map[string]TimeseriesConfig
	ListenAddr       string
	StaticDir        string
	ID               string
	ExposedPort      int
	RowTTLSec        int64
	DBPath           string
}

func (s *SqliteSink) registerTimeseriesTable(tableName string, parts []string) error {
	if len(parts) < 4 {
		return fmt.Errorf("invalid timeseries table name format")
	}

	tsType := TimeseriesType(parts[1])
	interval, err := time.ParseDuration(parts[3])
	if err != nil {
		return fmt.Errorf("failed to parse interval: %v", err)
	}

	var query string

	switch tsType {
	case GaugeType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			value REAL,
			metadata JSON,
			PRIMARY KEY (timestamp)
		)`, tableName)
	case CounterType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			count INTEGER,
			increment INTEGER,
			metadata JSON,
			PRIMARY KEY (timestamp)
		)`, tableName)
	case RollupType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			min REAL,
			max REAL,
			avg REAL,
			count INTEGER,
			metadata JSON,
			PRIMARY KEY (timestamp)
		)`, tableName)
	case DeltaType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			previous_value REAL,
			current_value REAL,
			delta REAL,
			metadata JSON,
			PRIMARY KEY (timestamp)
		)`, tableName)
	case SnapshotType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			value TEXT,
			count INTEGER,
			metadata JSON,
			PRIMARY KEY (timestamp, value)
		)`, tableName)
	default:
		return fmt.Errorf("unknown timeseries type: %s", tsType)
	}

	_, err = s.DB.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create timeseries table: %v", err)
	}

	viewQuery := fmt.Sprintf(`CREATE VIEW IF NOT EXISTS v_%s_latest AS
		SELECT * FROM %s
		WHERE timestamp = (
			SELECT MAX(timestamp) FROM %s
		)`, tableName, tableName, tableName)
	_, err = s.DB.Exec(viewQuery)
	if err != nil {
		return fmt.Errorf("failed to create latest view: %v", err)
	}

	triggerQuery := fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS cleanup_%s
		AFTER INSERT ON %s
		BEGIN
			DELETE FROM %s
			WHERE timestamp < NEW.timestamp - 86400;
		END;`, tableName, tableName, tableName)
	_, err = s.DB.Exec(triggerQuery)
	if err != nil {
		return fmt.Errorf("failed to create cleanup trigger: %v", err)
	}

	s.TimeseriesTables[tableName] = TimeseriesConfig{
		Type:      tsType,
		TableName: tableName,
		Interval:  interval,
	}

	return nil
}

func (s *SqliteSink) Init(config playerplugin.SinkConfig) error {
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
	s.TimeseriesTables = make(map[string]TimeseriesConfig)
	s.loadTableSchemas()

	for table := range s.TableSchemas {
		err = s.ensureColumnExists(table, "_rpf_last_updated_idx", "INTEGER")
		if err != nil {
			return fmt.Errorf("failed to add _rpf_last_updated_idx column: %v", err)
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

func (s *SqliteSink) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("/%s/rpf-db/", s.ID), s.handleGetRequest)
	mux.HandleFunc(fmt.Sprintf("/%s/query-table/", s.ID), s.handleGetRequest)
	mux.HandleFunc(fmt.Sprintf("/%s/list", s.ID), s.handleListTables)
	mux.HandleFunc(fmt.Sprintf("/%s/columns/", s.ID), s.handleListColumns)

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

func (s *SqliteSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels map[string]chan string) error {
	table := strings.TrimPrefix(path, "/rpf-db/")
	table = strings.TrimPrefix(table, "/")

	if !isValidTableName(table) {
		return fmt.Errorf("invalid table name: %s - only alphanumeric characters, underscore and dash are allowed", table)
	}

	if strings.HasPrefix(table, "ts_") {
		if _, exists := s.TimeseriesTables[table]; !exists {
			parts := strings.Split(table, "_")
			err := s.registerTimeseriesTable(table, parts)
			if err != nil {
				return err
			}
		}
	} else {
		err := s.ensureTableExists(table)
		if err != nil {
			return err
		}
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

		if strings.HasSuffix(column, "idx") || strings.HasSuffix(column, "index") || strings.HasSuffix(column, "id") || strings.HasSuffix(column, "fk") {
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

	if strings.HasPrefix(table, "ts_") {
		timestamp := time.Now().Unix()
		if t, ok := data["timestamp"]; ok {
			timestamp = t.(int64)
		}

		if tsConfig, ok := s.TimeseriesTables[table]; ok {
			// Round timestamp to nearest interval
			interval := int64(tsConfig.Interval.Seconds())
			timestamp = (timestamp / interval) * interval
			data["timestamp"] = timestamp

			switch tsConfig.Type {
			case CounterType:
				if value, ok := data["value"]; ok {
					var currentCount int64 = 0
					row := s.DB.QueryRow(fmt.Sprintf("SELECT count FROM %s WHERE timestamp = ?", table), data["timestamp"])
					err := row.Scan(&currentCount)
					if err != nil {
						if err == sql.ErrNoRows {
							data["count"] = int64(value.(float64))
						} else {
							return fmt.Errorf("failed to query current count: %v", err)
						}
					} else {
						data["count"] = currentCount + int64(value.(float64))
					}
					data["increment"] = value
				}
			case RollupType:
				if value, ok := data["value"]; ok {
					var min, max, sum float64 = 0, 0, 0
					var count int64 = 0
					row := s.DB.QueryRow(fmt.Sprintf("SELECT min, max, avg * count, count FROM %s WHERE timestamp = ?", table), data["timestamp"])
					err := row.Scan(&min, &max, &sum, &count)
					if err != nil {
						if err == sql.ErrNoRows {
							data["min"] = value
							data["max"] = value
							data["avg"] = value
							data["count"] = 1
						} else {
							return fmt.Errorf("failed to query rollup values: %v", err)
						}
					} else {
						v := value.(float64)
						if v < min {
							data["min"] = v
						} else {
							data["min"] = min
						}
						if v > max {
							data["max"] = v
						} else {
							data["max"] = max
						}
						data["avg"] = (sum + v) / float64(count+1)
						data["count"] = count + 1
					}
				}
			case DeltaType:
				if value, ok := data["value"]; ok {
					var previousValue float64 = 0
					row := s.DB.QueryRow(fmt.Sprintf("SELECT current_value FROM %s WHERE timestamp = (SELECT MAX(timestamp) FROM %s WHERE timestamp < ?)", table, table), data["timestamp"])
					err := row.Scan(&previousValue)
					if err != nil && err != sql.ErrNoRows {
						return fmt.Errorf("failed to query previous value: %v", err)
					}
					currentValue := value.(float64)
					data["previous_value"] = previousValue
					data["current_value"] = currentValue
					data["delta"] = currentValue - previousValue
				}
			case SnapshotType:
				if value, ok := data["value"]; ok {
					var count int64 = 0
					row := s.DB.QueryRow(fmt.Sprintf("SELECT count FROM %s WHERE timestamp = ? AND value = ?", table), data["timestamp"], value)
					err := row.Scan(&count)
					if err != nil {
						if err == sql.ErrNoRows {
							data["count"] = 1
						} else {
							return fmt.Errorf("failed to query snapshot count: %v", err)
						}
					} else {
						data["count"] = count + 1
					}
				}
			}
		}
	} else {
		if _, ok := data["id"]; !ok {
			return fmt.Errorf("id is mandatory")
		}
		data["_rpf_last_updated_idx"] = time.Now().Unix()
	}

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

	if strings.HasPrefix(table, "ts_") {
		timestamp, ok := data["timestamp"]
		if !ok {
			return fmt.Errorf("timestamp is required for timeseries delete")
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE timestamp = ?", table)
		_, err = s.DB.Exec(query, timestamp)
	} else {
		id, ok := data["id"]
		if !ok {
			return fmt.Errorf("id is required for delete")
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", table)
		_, err = s.DB.Exec(query, id)
	}

	if err != nil {
		return fmt.Errorf("failed to delete: %v", err)
	}
	return nil
}
func (s *SqliteSink) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	table := strings.TrimPrefix(r.URL.Path, fmt.Sprintf("/%s/rpf-db/", s.ID))
	table = strings.TrimPrefix(table, fmt.Sprintf("/%s/query-table/", s.ID))
	query := fmt.Sprintf("SELECT * FROM %s", table)

	var conditions []string
	var args []interface{}
	orderByClause := ""
	limitClause := ""

	for key, values := range r.URL.Query() {
		if key == "order" {
			orderByClause = fmt.Sprintf(" ORDER BY %s", values[0])
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
	return reg.ReplaceAllString(input, "")
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
