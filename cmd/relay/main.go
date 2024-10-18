package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	configPath          string
	dbFolder            = "./"
	dbMaxSizeKb         int
	tlS                 int
	maxDbSize           = 100 * 1024 * 1024 // 100 MB
	bucketAuth          map[string]string
	port                int
)

type Config struct {
	BucketAuth   map[string]string `json:"bucketAuth"`
	DbMaxSizeKb  int               `json:"dbMaxSizeKb"`
	Port         int               `json:"port"`
	TlS          int               `json:"tlS"`
}

func init() {
	flag.StringVar(&configPath, "c", "", "Path to config file")
	flag.IntVar(&dbMaxSizeKb, "m", 100000, "Maximum database size in KB")
	flag.IntVar(&port, "p", 8081, "Port to listen on")
	flag.IntVar(&tlS, "t", 0, "Time to live in seconds for records (0 for no delay)")

	// Add a new flag for bucket authentication
	var bucketAuthArg string
	flag.StringVar(&bucketAuthArg, "b", "", "Bucket authentication in the format 'bucket1:auth1,bucket2:auth2'")

	flag.Parse()

	// Parse the bucket authentication argument
	if bucketAuthArg != "" {
		bucketAuth = make(map[string]string)
		pairs := strings.Split(bucketAuthArg, ",")
		for _, pair := range pairs {
			kv := strings.Split(pair, ":")
			if len(kv) == 2 {
				bucketAuth[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
			}
		}
	}
}

func main() {
	var config Config

	if configPath != "" {
		log.Printf("Config path: %s", configPath)
		data, err := os.ReadFile(configPath)
		if err != nil {
			log.Fatalf("Failed to read config file: %v", err)
		}
		if err := json.Unmarshal(data, &config); err != nil {
			log.Fatalf("Failed to parse config file: %v", err)
		}
		bucketAuth = config.BucketAuth
		if config.DbMaxSizeKb > 0 {
			dbMaxSizeKb = config.DbMaxSizeKb
		}
		if config.Port > 0 {
			port = config.Port
		}
		if config.TlS >= 0 {
			tlS = config.TlS
		}
	}

	if len(bucketAuth) == 0 {
		log.Fatal("At least one bucket authentication is required")
	}

	log.Printf("Configuration:")
	log.Printf("  Config Path: %s", configPath)
	log.Printf("  DB Max Size: %d KB", dbMaxSizeKb)
	log.Printf("  Port: %d", port)
	log.Printf("  Time to Live: %d seconds", tlS)
	log.Printf("  Buckets:")
	for bucket := range bucketAuth {
		log.Printf("    - %s", bucket)
	}

	// Main server loop would go here
	http.HandleFunc("/record", handleRecord)
	http.HandleFunc("/first", handleFirst)
	http.HandleFunc("/acknowledge", handleAcknowledge)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func verifyAuth(authHeader, bucket string) bool {
	if authHeader != "" {
		if bearer := authHeader[7:]; bearer == bucketAuth[bucket] {
			return true
		}
	}
	return false
}

func handleRecord(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		log.Printf("Method not allowed: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Failed to read body: %v", err)
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}

	buckets := strings.Split(r.Header.Get("RF-BUCKETS"), ",")
	for _, bucket := range buckets {
		bucket = strings.TrimSpace(bucket)
		if !verifyAuth(r.Header.Get("Authorization"), bucket) {
			log.Printf("Authentication failed for bucket: %s", bucket)
			http.Error(w, "Authentication failed", http.StatusUnauthorized)
			return
		}

		dbFolderBucket := filepath.Join(dbFolder, bucket)
		dbPath := filepath.Join(dbFolderBucket, "relay.sqlite3")

		stat, err := os.Stat(dbPath)
		if err == nil && stat.Size() > int64(dbMaxSizeKb*1024/2) {
			log.Printf("Database size limit exceeded for bucket: %s", bucket)
			http.Error(w, "Database size limit exceeded", http.StatusInternalServerError)
			return
		}

		err = os.MkdirAll(dbFolderBucket, 0755)
		if err != nil {
			log.Printf("Failed to create database folder for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to create database folder", http.StatusInternalServerError)
			return
		}

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			log.Printf("Failed to open database for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to open database", http.StatusInternalServerError)
			return
		}
		defer db.Close()

		_, err = db.Exec("PRAGMA journal_mode=WAL")
		if err != nil {
			log.Printf("Failed to set journal mode for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to set journal mode", http.StatusInternalServerError)
			return
		}

		_, err = db.Exec("PRAGMA synchronous=NORMAL")
		if err != nil {
			log.Printf("Failed to set synchronous mode for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to set synchronous mode", http.StatusInternalServerError)
			return
		}

		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS wrap_calls (
							id INTEGER PRIMARY KEY,
							content TEXT,
							timestamp INTEGER
			);
			CREATE INDEX IF NOT EXISTS idx_timestamp ON wrap_calls (timestamp);
								`)
		if err != nil {
			log.Printf("Failed to create table or index for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to create table or index", http.StatusInternalServerError)
			return
		}

		_, err = db.Exec("INSERT INTO wrap_calls (content, timestamp) VALUES (?, ?)", string(body), time.Now().Unix())
		if err != nil {
			log.Printf("Failed to insert data for bucket %s: %v", bucket, err)
			http.Error(w, "Failed to insert data", http.StatusInternalServerError)
			return
		}

		// Delete expired records if tlS > 0
		if tlS > 0 {
			_, err = db.Exec("DELETE FROM wrap_calls WHERE timestamp < ?", time.Now().Unix()-int64(tlS))
			if err != nil {
				log.Printf("Failed to delete expired records for bucket %s: %v", bucket, err)
			}
		}
	}

	w.WriteHeader(http.StatusOK)
}

func handleFirst(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		log.Printf("Method not allowed: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.Header.Get("RF-BUCKET")
	if bucket == "" {
		log.Printf("Missing RF-BUCKET header")
		http.Error(w, "Missing RF-BUCKET header", http.StatusBadRequest)
		return
	}

	if !verifyAuth(r.Header.Get("Authorization"), bucket) {
		log.Printf("Authentication failed for bucket: %s", bucket)
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	dbPath := filepath.Join(dbFolder, bucket, "relay.sqlite3")

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Printf("No content found for bucket: %s", bucket)
		http.Error(w, "No content found", http.StatusNotFound)
		return
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Printf("Failed to open database: %v", err)
		http.Error(w, "Failed to open database", http.StatusInternalServerError)
		return
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		log.Printf("Failed to set journal mode: %v", err)
		http.Error(w, "Failed to set journal mode", http.StatusInternalServerError)
		return
	}

	_, err = db.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		log.Printf("Failed to set synchronous mode: %v", err)
		http.Error(w, "Failed to set synchronous mode", http.StatusInternalServerError)
		return
	}

	var id int
	var content string
	err = db.QueryRow(`
								SELECT id, content
								FROM wrap_calls
								WHERE id = (SELECT MIN(id) FROM wrap_calls)
				`).Scan(&id, &content)

	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("{}"))
		return
	} else if err != nil {
		log.Printf("Failed to query database: %v", err)
		http.Error(w, "Failed to query database", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"id":      id,
		"content": content,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func handleAcknowledge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		log.Printf("Method not allowed: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bucket := r.Header.Get("RF-BUCKET")
	if bucket == "" {
		log.Printf("Missing RF-BUCKET header")
		http.Error(w, "Missing RF-BUCKET header", http.StatusBadRequest)
		return
	}

	if !verifyAuth(r.Header.Get("Authorization"), bucket) {
		log.Printf("Authentication failed for bucket: %s", bucket)
		http.Error(w, "Authentication failed", http.StatusUnauthorized)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		log.Printf("Missing id parameter")
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	dbPath := filepath.Join(dbFolder, bucket, "relay.sqlite3")

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Printf("No content found for bucket: %s", bucket)
		http.Error(w, "No content found", http.StatusNotFound)
		return
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Printf("Failed to open database: %v", err)
		http.Error(w, "Failed to open database", http.StatusInternalServerError)
		return
	}
	defer db.Close()

	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		log.Printf("Failed to set journal mode: %v", err)
		http.Error(w, "Failed to set journal mode", http.StatusInternalServerError)
		return
	}

	_, err = db.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		log.Printf("Failed to set synchronous mode: %v", err)
		http.Error(w, "Failed to set synchronous mode", http.StatusInternalServerError)
		return
	}

	result, err := db.Exec("DELETE FROM wrap_calls WHERE id = ?", id)
	if err != nil {
		log.Printf("Failed to delete record: %v", err)
		http.Error(w, "Failed to delete record", http.StatusInternalServerError)
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		log.Printf("No record found with id: %s", id)
		http.Error(w, "No record found with the given id", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Record deleted successfully"))
}
