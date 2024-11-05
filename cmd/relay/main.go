package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chamot1111/replayforge/internal/envparser"
	_ "github.com/mattn/go-sqlite3"
	"tailscale.com/tsnet"
	"github.com/chamot1111/replayforge/pkgs/logger"
)

var (
	configPath string
	dbFolder   = "./"
	port       int
	config     Config
)

type BucketConfig struct {
	Auth        string `json:"auth"`
	DbMaxSizeKb int    `json:"dbMaxSizeKb"`
	Tls         int    `json:"tls"`
}

type Config struct {
	Buckets         map[string]BucketConfig `json:"buckets"`
	Port            int                     `json:"port"`
	UseTailnet      bool                    `json:"useTailnet"`
	TailnetHostname string                  `json:"tailnetHostname,omitempty"`
}

func init() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel != "" {
		logger.SetLogLevel(logLevel)
	}

	flag.StringVar(&configPath, "c", "", "Path to config file")
	flag.IntVar(&port, "p", 8081, "Port to listen on")
	flag.Parse()
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

	logger.Info("Configuration:")
	logger.Info("  Config Path: %s", configPath)
	logger.Info("  Port: %d", port)
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

	for _, bucket := range buckets {
		bucket = strings.TrimSpace(bucket)
		if !verifyAuth(r.Header.Get("Authorization"), bucket) {
			logger.Warn("Authentication failed for bucket: %s", bucket)
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

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
