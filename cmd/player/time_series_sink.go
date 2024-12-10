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

	"sync"

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

type TimeseriesSink struct {
	DB               *sql.DB
	TimeseriesTables map[string]TimeseriesConfig
	ListenAddr       string
	ExposedPort      int
	RowTTLSec        int64
	DBPath           string
	LastRpfID        int64
	ID               string
}

func (s *TimeseriesSink) Init(config playerplugin.SinkConfig, sinkChannels *sync.Map) error {
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

	s.DBPath = dbPath + "?_auto_vacuum=1&_journal_mode=WAL&_synchronous=NORMAL"

	listenPort, ok := params["listen_port"].(string)
	if !ok {
		return fmt.Errorf("listen_port not found in params or not a string")
	}
	port, err := strconv.Atoi(listenPort)
	if err != nil {
		return fmt.Errorf("failed to parse listen_port as integer: %v", err)
	}
	s.ExposedPort = port

	listenHost, ok := params["listen_host"].(string)
	if !ok {
		listenHost = "localhost"
	}

	s.ListenAddr = fmt.Sprintf("%s:%s", listenHost, listenPort)

	db, err := sql.Open("sqlite3", s.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	s.DB = db
	s.ID = config.ID
	s.TimeseriesTables = make(map[string]TimeseriesConfig)

	return nil
}

func (s *TimeseriesSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels *sync.Map) error {
	table := strings.TrimPrefix(path, "/")

	if !isValidTableName(table) {
		return fmt.Errorf("invalid table name: %s", table)
	}

	if _, exists := s.TimeseriesTables[table]; !exists {
		parts := strings.Split(table, "_")
		err := s.registerTimeseriesTable(table, parts)
		if err != nil {
			return err
		}
	}

	switch method {
	case "POST", "PUT":
		return s.handlePost(table, body)
	default:
		return fmt.Errorf("unsupported method: %s", method)
	}
}

func (s *TimeseriesSink) Close() error {
	return s.DB.Close()
}

func (s *TimeseriesSink) GetID() string {
	return "timeseries"
}

func (s *TimeseriesSink) GetExposedPort() (int, bool) {
	return s.ExposedPort, true
}
func (s *TimeseriesSink) registerTimeseriesTable(tableName string, parts []string) error {
	offset := 0
	if len(parts) > 0 && parts[0] == "ts" {
		offset = 1
	}

	if len(parts) < 3+offset {
		return fmt.Errorf("invalid timeseries table name format")
	}

	tsType := TimeseriesType(parts[offset])
	interval, err := time.ParseDuration(parts[2+offset])
	if err != nil {
		return fmt.Errorf("failed to parse interval: %v", err)
	}

	var query string
	switch tsType {
	case GaugeType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			serie_name TEXT NOT NULL,
			env_name TEXT NOT NULL,
			value REAL,
			metadata JSON,
			_rpf_id INTEGER,
			_rpf_last_updated_idx INTEGER,
			PRIMARY KEY (timestamp, serie_name, env_name)
		)`, tableName)
	case CounterType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			serie_name TEXT NOT NULL,
			env_name TEXT NOT NULL,
			count INTEGER,
			increment INTEGER,
			metadata JSON,
			_rpf_id INTEGER,
			_rpf_last_updated_idx INTEGER,
			PRIMARY KEY (timestamp, serie_name, env_name)
		)`, tableName)
	case RollupType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			serie_name TEXT NOT NULL,
			env_name TEXT NOT NULL,
			min REAL,
			max REAL,
			avg REAL,
			count INTEGER,
			metadata JSON,
			_rpf_id INTEGER,
			_rpf_last_updated_idx INTEGER,
			PRIMARY KEY (timestamp, serie_name, env_name)
		)`, tableName)
	case DeltaType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			serie_name TEXT NOT NULL,
			env_name TEXT NOT NULL,
			previous_value REAL,
			current_value REAL,
			delta REAL,
			metadata JSON,
			_rpf_id INTEGER,
			_rpf_last_updated_idx INTEGER,
			PRIMARY KEY (timestamp, serie_name, env_name)
		)`, tableName)
	case SnapshotType:
		query = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER NOT NULL,
			serie_name TEXT NOT NULL,
			env_name TEXT NOT NULL,
			value TEXT,
			count INTEGER,
			metadata JSON,
			_rpf_id INTEGER,
			_rpf_last_updated_idx INTEGER,
			PRIMARY KEY (timestamp, serie_name, env_name, value)
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

func (s *TimeseriesSink) handlePost(table string, body []byte) error {
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

	timestamp := normalizeTimestamp(data["timestamp"])
	data["timestamp"] = timestamp

	if tsConfig, ok := s.TimeseriesTables[table]; ok {
		// Round timestamp to nearest interval
		interval := int64(tsConfig.Interval.Seconds())
		timestamp = (timestamp / interval) * interval
		data["timestamp"] = timestamp

		if _, ok := data["serie_name"]; !ok {
			data["serie_name"] = "default"
		}

		switch tsConfig.Type {
		case CounterType:
			if value, ok := data["value"]; ok {
				var currentCount sql.NullInt64
				row := s.DB.QueryRow(fmt.Sprintf("SELECT count FROM %s WHERE timestamp = ? AND serie_name = ?", table), data["timestamp"], data["serie_name"])
				err := row.Scan(&currentCount)
				if err != nil {
					if err == sql.ErrNoRows {
						data["count"] = int64(value.(float64))
					} else {
						return fmt.Errorf("failed to query current count: %v", err)
					}
				} else {
					if currentCount.Valid {
						data["count"] = currentCount.Int64 + int64(value.(float64))
					} else {
						data["count"] = int64(value.(float64))
					}
				}
				data["increment"] = value
			}
		case RollupType:
			if value, ok := data["value"]; ok {
				var min, max, sum float64 = 0, 0, 0
				var count int64 = 0
				row := s.DB.QueryRow(fmt.Sprintf("SELECT min, max, avg * count, count FROM %s WHERE timestamp = ? AND serie_name = ?", table), data["timestamp"], data["serie_name"])
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
				row := s.DB.QueryRow(fmt.Sprintf("SELECT current_value FROM %s WHERE serie_name = ? AND timestamp = (SELECT MAX(timestamp) FROM %s WHERE timestamp < ? AND serie_name = ?)", table, table), data["serie_name"], data["timestamp"], data["serie_name"])
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
				row := s.DB.QueryRow(fmt.Sprintf("SELECT count FROM %s WHERE timestamp = ? AND serie_name = ? AND value = ?", table), data["timestamp"], data["serie_name"], value)
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
	// Get table info to determine existing columns
	rows, err := s.DB.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("failed to get table info: %v", err)
	}
	defer rows.Close()

	existingColumns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name string
		var dtype string
		var notnull int
		var dfltValue interface{}
		var pk int
		err := rows.Scan(&cid, &name, &dtype, &notnull, &dfltValue, &pk)
		if err != nil {
			return fmt.Errorf("failed to scan table info: %v", err)
		}
		existingColumns[name] = true
	}

	columns := make([]string, 0)
	values := make([]interface{}, 0)
	placeholders := make([]string, 0)
	metadata := make(map[string]interface{})

	for k, v := range data {
		if existingColumns[k] {
			columns = append(columns, k)
			values = append(values, v)
			placeholders = append(placeholders, "?")
		} else if k != "metadata" {
			metadata[k] = v
		}
	}

	if len(metadata) > 0 {
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %v", err)
		}
		columns = append(columns, "metadata")
		values = append(values, string(metadataJSON))
		placeholders = append(placeholders, "?")
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

func (s *TimeseriesSink) handleGetRequest(w http.ResponseWriter, r *http.Request) {
	table := strings.TrimPrefix(r.URL.Path, fmt.Sprintf("/%s/query-table/", s.ID))
	if !isValidTableName(table) {
		http.Error(w, "Invalid table name", http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("SELECT * FROM %s", table)
	var conditions []string
	var args []interface{}
	orderByClause := " ORDER BY timestamp DESC"
	limitClause := " LIMIT 10000"

	for key, values := range r.URL.Query() {
		if key == "limit" {
			limitClause = fmt.Sprintf(" LIMIT %s", values[0])
		} else if key == "env_name" || key == "serie_name" {
			conditions = append(conditions, fmt.Sprintf("%s = ?", key))
			args = append(args, values[0])
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

func (s *TimeseriesSink) handleListAllTablesColumns(w http.ResponseWriter, r *http.Request) {
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

func (s *TimeseriesSink) handleDisinctValuesForTableColumns(w http.ResponseWriter, r *http.Request) {
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

func (s *TimeseriesSink) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("/%s/query-table/", s.ID), s.handleGetRequest)
	mux.HandleFunc(fmt.Sprintf("/%s/all-columns/", s.ID), s.handleListAllTablesColumns)
	mux.HandleFunc(fmt.Sprintf("/%s/distinct-values/", s.ID), s.handleDisinctValuesForTableColumns)

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
