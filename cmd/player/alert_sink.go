package main
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/chamot1111/replayforge/pkgs/lualibs"
	"github.com/chamot1111/replayforge/pkgs/playerplugin"

	"github.com/Shopify/go-lua"
	_ "github.com/mattn/go-sqlite3"
	"sync"
)

const ConsecutiveBadEventsThreshold = 3

type AlertSink struct {
	DB                 *sql.DB
	ListenAddr         string
	TableSchemas       map[string]map[string]string
	TableToLuaScript   map[string]string
	TableHeartbeatIntv map[string]time.Duration
	LastRpfID          int64
	LState             *lua.State
	DBPath             string
	RowTTLSec          int64
	ID                 string
	ExposedPort        int
	NotificationSink   string
	SinkChannels       *sync.Map
	ConsecutiveBadEvents map[string]int
	LastNotificationSent map[string]bool
	HeartbeatNotificationSent map[string]bool
}

func (s *AlertSink) Init(config playerplugin.SinkConfig, sinkChannels *sync.Map) error {
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

	listenAddr, ok := params["listen_addr"].(string)
	if !ok {
		return fmt.Errorf("listen_addr not found in params or not a string")
	}
	s.ListenAddr = listenAddr

	port, ok := params["exposed_port"].(float64)
	if !ok {
		return fmt.Errorf("exposed_port not found in params or not a number")
	}
	s.ExposedPort = int(port)

	if notificationSink, ok := params["notification_sink"].(string); ok {
		s.NotificationSink = notificationSink
	}

	scripts, ok := params["scripts"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("scripts mapping not found in params")
	}

	s.TableToLuaScript = make(map[string]string)
	s.TableHeartbeatIntv = make(map[string]time.Duration)
	s.ConsecutiveBadEvents = make(map[string]int)
	s.LastNotificationSent = make(map[string]bool)
	s.HeartbeatNotificationSent = make(map[string]bool)

	for table, script := range scripts {
		scriptConfig, ok := script.(map[string]interface{})
		if !ok {
			return fmt.Errorf("script config must be a map for table %s", table)
		}

		scriptPath, ok := scriptConfig["scriptPath"].(string)
		if !ok {
			return fmt.Errorf("scriptPath must be a string for table %s", table)
		}
		s.TableToLuaScript[table] = scriptPath

		heartbeatIntv, ok := scriptConfig["heartbeatInterval"].(string)
		if !ok {
			return fmt.Errorf("heartbeatInterval must be a string for table %s", table)
		}
		duration, err := time.ParseDuration(heartbeatIntv)
		if err != nil {
			return fmt.Errorf("invalid heartbeatInterval for table %s: %v", table, err)
		}
		s.TableHeartbeatIntv[table] = duration
		s.ConsecutiveBadEvents[table] = 0
		s.LastNotificationSent[table] = false
		s.HeartbeatNotificationSent[table] = false
	}

	db, err := sql.Open("sqlite3", s.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	s.DB = db
	s.ID = config.ID
	s.TableSchemas = make(map[string]map[string]string)
	s.LState = lua.NewState()
	lua.OpenLibraries(s.LState)
	s.LastRpfID = 0
	s.SinkChannels = sinkChannels

	s.loadTableSchemas()

	if s.RowTTLSec > 0 {
		go s.ttlCleanup()
	}

	return nil
}

func (s *AlertSink) ensureTableExists(table string) error {
	if _, exists := s.TableSchemas[table]; !exists {
		query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
												_rpf_id INTEGER PRIMARY KEY,
												_rpf_last_updated_idx INTEGER,
												status TEXT
								)`, table)
		_, err := s.DB.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %v", table, err)
		}
		s.TableSchemas[table] = make(map[string]string)
		s.TableSchemas[table]["_rpf_id"] = "INTEGER"
		s.TableSchemas[table]["_rpf_last_updated_idx"] = "INTEGER"
		s.TableSchemas[table]["status"] = "TEXT"
	}
	return nil
}

func (s *AlertSink) extractStatus(table string, data map[string]interface{}) (string, error) {
	scriptPath, ok := s.TableToLuaScript[table]
	if !ok {
		status, ok := data["status"].(string)
		if !ok {
			logger.Error("No script found for table %s and no status in data", table)
			return "error", nil
		}
		return status, nil
	}

	s.LState = lua.NewState()
	lua.OpenLibraries(s.LState)
	lualibs.RegisterLuaLibs(s.LState)

	script, err := os.ReadFile(scriptPath)
	if err != nil {
		return "", fmt.Errorf("failed to read lua script: %v", err)
	}

	if err := lua.DoString(s.LState, string(script)); err != nil {
		return "", fmt.Errorf("failed to load lua script: %v", err)
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data to JSON: %v", err)
	}

	s.LState.PushString(string(jsonBytes))
	s.LState.Global("extractStatus")
	if err := s.LState.ProtectedCall(1, 1, 0); err != nil {
		return "", fmt.Errorf("failed to call extract_status: %v", err)
	}

	status, _ := s.LState.ToString(-1)
	s.LState.Pop(1)

	if status == "" {
		status = "ok"
	}

	return status, nil
}

func (s *AlertSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels *sync.Map) error {
	// Remove leading slash
	table := strings.TrimPrefix(path, "/")

	if !isValidTableName(table) {
		return fmt.Errorf("invalid table name: %s", table)
	}

	err := s.ensureTableExists(table)
	if err != nil {
		return err
	}

	switch method {
	case "POST", "PUT":
		var data map[string]interface{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %v", err)
		}

		status, err := s.extractStatus(table, data)
		if err != nil {
			return fmt.Errorf("failed to extract status: %v", err)
		}

		// Reset heartbeat notification flag since we received a message
		s.HeartbeatNotificationSent[table] = false

		if status != "ok" {
			s.ConsecutiveBadEvents[table]++
			if s.ConsecutiveBadEvents[table] >= ConsecutiveBadEventsThreshold && !s.LastNotificationSent[table] {
				s.sendNotification(
					fmt.Sprintf("Status not OK for %s", table),
					fmt.Sprintf("Status %s detected for table %s", status, table),
				)
				s.LastNotificationSent[table] = true
			}
		} else {
			s.ConsecutiveBadEvents[table] = 0
			s.LastNotificationSent[table] = false
		}

		data["_rpf_last_updated_idx"] = time.Now().Unix()
		s.LastRpfID++
		data["_rpf_id"] = s.LastRpfID
		data["status"] = status

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
			return fmt.Errorf("failed to insert: %v", err)
		}

	default:
		return fmt.Errorf("unsupported method: %s", method)
	}

	return nil
}

func (s *AlertSink) ensureColumnExists(table, column, dataType string) error {
	if _, exists := s.TableSchemas[table][column]; !exists {
		query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, dataType)
		_, err := s.DB.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to add column %s to table %s: %v", column, table, err)
		}
		s.TableSchemas[table][column] = dataType
	}
	return nil
}

func (s *AlertSink) ttlCleanup() {
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

func (s *AlertSink) loadTableSchemas() {
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

func (s *AlertSink) Close() error {
	//s.LState.Close()
	return s.DB.Close()
}

func (s *AlertSink) GetID() string {
	return s.ID
}

func (s *AlertSink) GetExposedPort() (int, bool) {
	return s.ExposedPort, true
}

func (s *AlertSink) sendNotification(subject, message string) {
	if s.NotificationSink != "" {
		event, err := BuildNotificationEvent(subject, message)
		if err != nil {
			logger.Error("Failed to build notification event: %v", err)
			return
		}

		if ch, ok := s.SinkChannels.Load(s.NotificationSink); ok {
			ch.(chan string) <- string(event)
		} else {
			logger.Error("Notification sink %s not found", s.NotificationSink)
		}
	}
}

func (s *AlertSink) Start() error {
	mux := http.NewServeMux()

	logger.Info("Starting HTTP server on %s", s.ListenAddr)

	// Start heartbeat monitors for each table
	for table, interval := range s.TableHeartbeatIntv {
		go func(tableName string, checkInterval time.Duration) {
			ticker := time.NewTicker(checkInterval)
			for range ticker.C {
				// Check last update time for this table
				var lastUpdated int64
				err := s.DB.QueryRow(fmt.Sprintf("SELECT MAX(_rpf_last_updated_idx) FROM %s", tableName)).Scan(&lastUpdated)
				if err != nil {
					logger.Error("Failed to check last update time for table %s: %v", tableName, err)
					continue
				}

				// Check if table hasn't been updated within the interval
				if time.Now().Unix()-lastUpdated > int64(checkInterval.Seconds()) && !s.HeartbeatNotificationSent[tableName] {
					logger.Info("No messages received for table %s in the last %v", tableName, checkInterval)
					s.sendNotification(
						fmt.Sprintf("No messages for %s", tableName),
						fmt.Sprintf("No messages received for table %s in the last %v", tableName, checkInterval),
					)
					s.HeartbeatNotificationSent[tableName] = true
				}
			}
		}(table, interval)
	}

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
