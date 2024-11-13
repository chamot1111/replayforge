package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chamot1111/replayforge/internal/envparser"
	"github.com/chamot1111/replayforge/version"
	_ "github.com/mattn/go-sqlite3"
	"tailscale.com/tsnet"
	"github.com/chamot1111/replayforge/pkgs/logger"
)

var (
	configPath string
	dbFolder   = "./"
	port       int
	config     Config
	startTime  = time.Now()
	statsMutex sync.RWMutex
	enablePprof bool
	nodeInfoStats = make(map[string]*NodeInfo)
)

type NodeInfo struct {
	MemoryProcess      float64   `json:"memoryProcess"`
	MemoryHostTotal    float64   `json:"memoryHostTotal"`
	MemoryHostFree     float64   `json:"memoryHostFree"`
	MemoryHostUsedPct  float64   `json:"memoryHostUsedPct"`
	CpuPercentHost   float64   `json:"cpuPercentHost"`
	LastUpdated time.Time `json:"lastUpdated"`
	WarnCount int `json:"warnCount"`
	ErrorCount int `json:"errorCount"`
}

type BucketConfig struct {
	Auth        string `json:"auth"`
	DbMaxSizeKb int    `json:"dbMaxSizeKb"`
	Tls         int    `json:"tls"`
}

type BucketStats struct {
	Kind                string    `json:"kind"`
	ID                  string    `json:"id"`
	RxMessagesByMinute  int       `json:"rxMessagesByMinute"`
	LastMinuteRxMessages int      `json:"lastMinuteRxMessages"`
	RxMessagesSinceStart int      `json:"rxMessagesSinceStart"`
	RxLastMessageDate   time.Time `json:"rxLastMessageDate"`
	RxQueryByMinute     int       `json:"rxQueryByMinute"`
	TxMessageByMinute   int       `json:"txMessageByMinute"`
	TxQueryByMinute     int       `json:"txQueryByMinute"`
	TxLastAccess        time.Time `json:"txLastAccess"`
}

type Config struct {
	Buckets         map[string]BucketConfig `json:"buckets"`
	Port            int                     `json:"port"`
	PortStatusZ     int                     `json:"portStatusZ"`
	UseTailnet      bool                    `json:"useTailnet"`
	TailnetHostname string                  `json:"tailnetHostname,omitempty"`
}

var bucketStats = make(map[string]*BucketStats)
var envStats = make(map[string]*BucketStats)
var hostnameStats = make(map[string]*BucketStats)

var statsResetTicker = time.NewTicker(time.Minute)

func init() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel != "" {
		logger.SetLogLevel(logLevel)
	}

	enablePprof = os.Getenv("ENABLE_PPROF") == "true"

	flag.StringVar(&configPath, "c", "", "Path to config file")
	flag.IntVar(&port, "p", 8081, "Port to listen on")
	versionFlag := flag.Bool("v", false, "Affiche la version")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("Version: %s\n", version.Version)
		os.Exit(0)
	}

	// Start goroutine to reset per-minute stats
	go func() {
		for range statsResetTicker.C {
			statsMutex.Lock()
			for _, stats := range bucketStats {
				stats.LastMinuteRxMessages = stats.RxMessagesByMinute
				stats.RxMessagesByMinute = 0
				stats.RxQueryByMinute = 0
				stats.TxMessageByMinute = 0
				stats.TxQueryByMinute = 0
			}
			for _, stats := range envStats {
				stats.LastMinuteRxMessages = stats.RxMessagesByMinute
				stats.RxMessagesByMinute = 0
				stats.RxQueryByMinute = 0
				stats.TxMessageByMinute = 0
				stats.TxQueryByMinute = 0
			}
			for _, stats := range hostnameStats {
				stats.LastMinuteRxMessages = stats.RxMessagesByMinute
				stats.RxMessagesByMinute = 0
				stats.RxQueryByMinute = 0
				stats.TxMessageByMinute = 0
				stats.TxQueryByMinute = 0
			}
			statsMutex.Unlock()
		}
	}()
}

func handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.Header.Get("RF-BUCKET")
	if bucket == "" {
		http.Error(w, "Missing RF-BUCKET header", http.StatusBadRequest)
		return
	}

	if !verifyAuth(r.Header.Get("Authorization"), bucket) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	hostname := r.Header.Get("RF-HOSTNAME")
	if hostname == "" {
		http.Error(w, "Missing RF-HOSTNAME header", http.StatusBadRequest)
		return
	}

	var info NodeInfo
	if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	info.LastUpdated = time.Now()

	statsMutex.Lock()
	nodeInfoStats[hostname] = &info
	statsMutex.Unlock()

	w.WriteHeader(http.StatusOK)
}

func handleStatusZ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	statsMutex.RLock()
	stats := struct {
		Uptime string                   `json:"uptime"`
		Buckets map[string]*BucketStats `json:"buckets"`
		Envs map[string]*BucketStats `json:"envs"`
		Hostnames map[string]*BucketStats `json:"hostnames"`
		NodeInfo map[string]*NodeInfo    `json:"nodeInfo"`
	}{
		Uptime:  time.Since(startTime).String(),
		Buckets: bucketStats,
		Envs: envStats,
		Hostnames: hostnameStats,
		NodeInfo: nodeInfoStats,
	}
	statsMutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func main() {
	if configPath == "" {
		logger.Fatal("Config file path is required")
	}

	logger.Info("Config path: %s", configPath)
	data, err := os.ReadFile(configPath)
	if err != nil {
		logger.Fatal("Failed to read config file: %v", err)
	}

	configDataStr, err := envparser.ProcessJSONWithEnvVars(string(data))
	if err != nil {
		logger.Fatal("Error processing environment variables in config: %v", err)
	}
	data = []byte(configDataStr)

	if err := json.Unmarshal(data, &config); err != nil {
		logger.Fatal("Failed to parse config file: %v", err)
	}

	if config.Port > 0 {
		port = config.Port
	}

	if len(config.Buckets) == 0 {
		logger.Fatal("At least one bucket configuration is required")
	}

	statsMutex.Lock()
	for bucket := range config.Buckets {
		bucketStats[bucket] = &BucketStats{
			Kind: "relay",
			ID:   bucket,
		}
	}
	statsMutex.Unlock()

	logger.Info("Configuration:")
	logger.Info("  Config Path: %s", configPath)
	logger.Info("  Port: %d", port)
	logger.Info("  StatusZ Port: %d", config.PortStatusZ)
	logger.Info("  Use Tailnet: %v", config.UseTailnet)
	logger.Info("  Buckets:")
	for bucket, bucketConfig := range config.Buckets {
		logger.Info("    - %s:", bucket)
		logger.Info("      DB Max Size: %d KB", bucketConfig.DbMaxSizeKb)
		logger.Info("      Time to Live: %d seconds", bucketConfig.Tls)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/record-batch", handleRecordBatch)
	mux.HandleFunc("/first-batch", handleFirstBatch)
	mux.HandleFunc("/acknowledge-batch", handleAcknowledgeBatch)
	mux.HandleFunc("/node-info", handleNodeInfo)

	// Start pprof server if enabled
	if enablePprof {
		go func() {
			logger.Info("Starting pprof server on :6060")
			logger.Error("pprof server error: %v", http.ListenAndServe("localhost:6060", nil))
		}()
	}

	// Start status server
	if config.PortStatusZ > 0 {
		go func() {
			statusMux := http.NewServeMux()
			statusMux.HandleFunc("/statusz", handleStatusZ)
			statusMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("ok"))
			})
			logger.Info("Status server listening on port %d", config.PortStatusZ)
			if err := http.ListenAndServe(fmt.Sprintf(":%d", config.PortStatusZ), statusMux); err != nil {
				logger.Fatal("Status server error: %v", err)
			}
		}()
	}

	if config.UseTailnet {
		hostname := "relay-forwarder"
		if config.TailnetHostname != "" {
			hostname = config.TailnetHostname
		}
		s := &tsnet.Server{
			Hostname: hostname,
		}
		defer s.Close()

		ln, err := s.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			logger.Fatal("Failed to listen on Tailscale network: %v", err)
		}
		defer ln.Close()

		logger.Info("Listening on Tailscale network on port %d", port)
		logger.Fatal("Server error: %v", http.Serve(ln, mux))
	} else {
		logger.Info("Listening on port %d", port)
		logger.Fatal("Server error: %v", http.ListenAndServe(fmt.Sprintf(":%d", port), mux))
	}
}

func verifyAuth(authHeader, bucket string) bool {
	if authHeader != "" {
		if len(authHeader) > 7 && strings.HasPrefix(authHeader, "Bearer ") {
			if bearer := authHeader[7:]; bearer == config.Buckets[bucket].Auth {
				return true
			}
		}
	}
	return false
}

func handleRecordBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.Warn("Method not allowed: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Failed to read body: %v", err)
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}

	var events []string
	if err := json.Unmarshal(body, &events); err != nil {
		logger.Error("Failed to parse events: %v", err)
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	buckets := strings.Split(r.Header.Get("RF-BUCKETS"), ",")
	exceededBuckets := []string{}

	envName := r.Header.Get("RF-ENV-NAME")
	hostname := r.Header.Get("RF-HOSTNAME")
	now := time.Now()

	for _, bucket := range buckets {
		bucket = strings.TrimSpace(bucket)
		if !verifyAuth(r.Header.Get("Authorization"), bucket) {
			logger.Warn("Authentication failed for bucket: %s", bucket)
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

		// Update bucket stats
		statsMutex.Lock()
		if stats, ok := bucketStats[bucket]; ok {
			stats.RxMessagesByMinute += len(events)
			stats.RxMessagesSinceStart += len(events)
			stats.RxLastMessageDate = now
			stats.RxQueryByMinute++
		}

		// Update env stats if env name provided
		if envName != "" {
			if stats, ok := envStats[envName]; ok {
				stats.RxMessagesByMinute += len(events)
				stats.RxMessagesSinceStart += len(events)
				stats.RxLastMessageDate = now
				stats.RxQueryByMinute++
			} else {
				envStats[envName] = &BucketStats{
					Kind: "env",
					ID: envName,
					RxMessagesByMinute: len(events),
					RxMessagesSinceStart: len(events),
					RxLastMessageDate: now,
					RxQueryByMinute: 1,
				}
			}
		}

		// Update hostname stats if hostname provided
		if hostname != "" {
			if stats, ok := hostnameStats[hostname]; ok {
				stats.RxMessagesByMinute += len(events)
				stats.RxMessagesSinceStart += len(events)
				stats.RxLastMessageDate = now
				stats.RxQueryByMinute++
			} else {
				hostnameStats[hostname] = &BucketStats{
					Kind: "hostname",
					ID: hostname,
					RxMessagesByMinute: len(events),
					RxMessagesSinceStart: len(events),
					RxLastMessageDate: now,
					RxQueryByMinute: 1,
				}
			}
		}
		statsMutex.Unlock()

		bucketConfig := config.Buckets[bucket]
		dbFolderBucket := filepath.Join(dbFolder, bucket)
		dbPath := filepath.Join(dbFolderBucket, "relay.sqlite3")

		stat, err := os.Stat(dbPath)
		if err == nil && stat.Size() > int64(bucketConfig.DbMaxSizeKb*1024/2) {
			logger.Warn("Database size limit exceeded for bucket: %s", bucket)
			exceededBuckets = append(exceededBuckets, bucket)
			continue
		}

		err = os.MkdirAll(dbFolderBucket, 0755)
		if err != nil {
			logger.Error("Failed to create database folder for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to create database folder", http.StatusInternalServerError)
			return
		}

		db, err := sql.Open("sqlite3", dbPath+"?_auto_vacuum=1&_journal_mode=WAL&_synchronous=NORMAL")
		if err != nil {
			logger.Error("Failed to open database for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to open database", http.StatusInternalServerError)
			return
		}
		defer db.Close()

		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS wrap_calls (
							id INTEGER PRIMARY KEY,
							content TEXT,
							timestamp INTEGER
			);
			CREATE INDEX IF NOT EXISTS idx_timestamp ON wrap_calls (timestamp);
								`)
		if err != nil {
			logger.Error("Failed to create table or index for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to create table or index", http.StatusInternalServerError)
			return
		}

		tx, err := db.Begin()
		if err != nil {
			logger.Error("Failed to begin transaction for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to begin transaction", http.StatusInternalServerError)
			return
		}

		stmt, err := tx.Prepare("INSERT INTO wrap_calls (content, timestamp) VALUES (?, ?)")
		if err != nil {
			tx.Rollback()
			logger.Error("Failed to prepare statement for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to prepare statement", http.StatusInternalServerError)
			return
		}
		defer stmt.Close()

		timestamp := time.Now().Unix()
		for _, event := range events {
			_, err = stmt.Exec(event, timestamp)
			if err != nil {
				tx.Rollback()
				logger.Error("Failed to insert event for bucket %s: %v", bucket, err)
				http.Error(w, "Failed to insert event", http.StatusInternalServerError)
				return
			}
		}

		if err = tx.Commit(); err != nil {
			logger.Error("Failed to commit transaction for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to commit transaction", http.StatusInternalServerError)
			return
		}

		if bucketConfig.Tls > 0 {
			_, err = db.Exec("DELETE FROM wrap_calls WHERE timestamp < ?", time.Now().Unix()-int64(bucketConfig.Tls))
			if err != nil {
				logger.Error("Failed to delete expired records for bucket %s: %v", bucket, err)
			}
		}
	}

	if len(exceededBuckets) > 0 {
		logger.Warn("Database size limit exceeded for buckets: %v", exceededBuckets)
		http.Error(w, fmt.Sprintf("Database size limit exceeded for buckets: %v", exceededBuckets), http.StatusTooManyRequests)
		return
	}

	w.WriteHeader(http.StatusOK)
}
func handleFirstBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.Warn("Method not allowed: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.Header.Get("RF-BUCKET")
	if bucket == "" {
		logger.Warn("Missing RF-BUCKET header")
		http.Error(w, "Missing RF-BUCKET header", http.StatusBadRequest)
		return
	}

	if !verifyAuth(r.Header.Get("Authorization"), bucket) {
		logger.Warn("Authentication failed for bucket: %s", bucket)
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	envName := r.Header.Get("RF-ENV-NAME")
	hostname := r.Header.Get("RF-HOSTNAME")
	now := time.Now()

	limitStr := r.URL.Query().Get("limit")
	limit := 100 // Default limit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil || parsedLimit <= 0 {
			logger.Warn("Invalid limit parameter: %s", limitStr)
			http.Error(w, "Invalid limit parameter", http.StatusBadRequest)
			return
		}
		if parsedLimit < limit {
			limit = parsedLimit
		}
	}

	dbPath := filepath.Join(dbFolder, bucket, "relay.sqlite3")

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		logger.Info("No content found for bucket: %s", bucket)
		http.Error(w, "No content found", http.StatusNotFound)
		return
	}

	db, err := sql.Open("sqlite3", dbPath+"?_auto_vacuum=1&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		logger.Error("Failed to open database: %v", err)
		http.Error(w, "Failed to open database", http.StatusInternalServerError)
		return
	}
	defer db.Close()

	rows, err := db.Query(`
								SELECT id, content
								FROM wrap_calls
								ORDER BY id ASC
								LIMIT ?
				`, limit)
	if err != nil {
		logger.Error("Failed to query database: %v", err)
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var id int
		var content string
		if err := rows.Scan(&id, &content); err != nil {
			logger.Error("Failed to scan row: %v", err)
			http.Error(w, "Failed to scan row", http.StatusInternalServerError)
			return
		}
		results = append(results, map[string]interface{}{
			"id":      id,
			"content": content,
		})
	}

	if len(results) == 0 {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("[]"))
		return
	}

	statsMutex.Lock()
	if stats, ok := bucketStats[bucket]; ok {
		stats.TxQueryByMinute++
		stats.TxMessageByMinute += len(results)
		stats.TxLastAccess = now
	}

	if envName != "" {
		if stats, ok := envStats[envName]; ok {
			stats.TxQueryByMinute++
			stats.TxMessageByMinute += len(results)
			stats.TxLastAccess = now
		} else {
			envStats[envName] = &BucketStats{
				Kind: "env",
				ID: envName,
				TxQueryByMinute: 1,
				TxMessageByMinute: len(results),
				TxLastAccess: now,
			}
		}
	}

	if hostname != "" {
		if stats, ok := hostnameStats[hostname]; ok {
			stats.TxQueryByMinute++
			stats.TxMessageByMinute += len(results)
			stats.TxLastAccess = now
		} else {
			hostnameStats[hostname] = &BucketStats{
				Kind: "hostname",
				ID: hostname,
				TxQueryByMinute: 1,
				TxMessageByMinute: len(results),
				TxLastAccess: now,
			}
		}
	}
	statsMutex.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
}

func handleAcknowledgeBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		logger.Warn("Method not allowed: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.Header.Get("RF-BUCKET")
	if bucket == "" {
		logger.Warn("Missing RF-BUCKET header")
		http.Error(w, "Missing RF-BUCKET header", http.StatusBadRequest)
		return
	}

	if !verifyAuth(r.Header.Get("Authorization"), bucket) {
		logger.Warn("Authentication failed for bucket: %s", bucket)
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("Failed to read body: %v", err)
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}

	var requestBody struct {
		Ids []string `json:"ids"`
	}
	if err := json.Unmarshal(body, &requestBody); err != nil {
		logger.Error("Failed to parse ids: %v", err)
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}

	if len(requestBody.Ids) == 0 {
		logger.Warn("Empty ids array")
		http.Error(w, "Empty ids array", http.StatusBadRequest)
		return
	}

	dbPath := filepath.Join(dbFolder, bucket, "relay.sqlite3")

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		logger.Info("No content found for bucket: %s", bucket)
		http.Error(w, "No content found", http.StatusNotFound)
		return
	}

	db, err := sql.Open("sqlite3", dbPath+"?_auto_vacuum=1&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		logger.Error("Failed to open database: %v", err)
		http.Error(w, "Failed to open database", http.StatusInternalServerError)
		return
	}
	defer db.Close()

	placeholders := strings.Repeat("?,", len(requestBody.Ids))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf("DELETE FROM wrap_calls WHERE id IN (%s)", placeholders)

	args := make([]interface{}, len(requestBody.Ids))
	for i, id := range requestBody.Ids {
		args[i] = id
	}

	result, err := db.Exec(query, args...)
	if err != nil {
		logger.Error("Failed to delete records: %v", err)
		http.Error(w, "Failed to delete records", http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		logger.Info("No records found with provided ids")
		http.Error(w, "No records found with provided ids", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d records deleted successfully", rowsAffected)))
}
