package main

import (
 "database/sql"
 "encoding/json"
 "flag"
 "log"
 "net/http"
 "os"
 "path/filepath"
 "strings"
 "syscall"
 "time"
 "io/ioutil"

 _ "github.com/mattn/go-sqlite3"
)

var (
 configPath         string
 dbFolder           = "./"
 heartbeatIntervalMs = 1000
 dbMaxSizeKb        = 100000
 tlS               = 3600
 maxDbSize          = 100 * 1024 * 1024 // 100 MB
 relayAuthenticationBearer string
)

type Config struct {
 RelayAuthenticationBearer string `json:"relayAuthenticationBearer"`
}

func init() {
 flag.StringVar(&configPath, "c", "", "Path to config file")
 flag.StringVar(&relayAuthenticationBearer, "a", "", "Relay authentication bearer token")
 flag.Parse()
}

func main() {
 var config Config

 if configPath != "" {
  log.Printf("Config path: %s", configPath)
  data, err := ioutil.ReadFile(configPath)
  if err != nil {
   log.Fatalf("Failed to read config file: %v", err)
  }
  if err := json.Unmarshal(data, &config); err != nil {
   log.Fatalf("Failed to parse config file: %v", err)
  }
  if config.RelayAuthenticationBearer != "" {
   relayAuthenticationBearer = config.RelayAuthenticationBearer
  }
 }

 if relayAuthenticationBearer == "" {
  log.Fatal("Relay authentication bearer token is required")
 }

 // Main server loop would go here
 http.HandleFunc("/record", handleRecord)
 http.HandleFunc("/first", handleFirst)
 http.HandleFunc("/acknowledge", handleAcknowledge)

 log.Fatal(http.ListenAndServe(":8081", nil))
}

func verifyAuth(authHeader string) bool {
 if authHeader != "" {
  if bearer := authHeader[7:]; bearer == relayAuthenticationBearer {
   return true
  }
 }
 return false
}

func permuteSqliteDb(db0, db1 string) error {
 fileExists := func(path string) bool {
  _, err := os.Stat(path)
  return !os.IsNotExist(err)
 }

 openIfExists := func(path string, flag int) (*os.File, error) {
  if fileExists(path) {
   return os.OpenFile(path, flag, 0)
  }
  return nil, nil
 }

 fd0, _ := openIfExists(db0, os.O_RDONLY)
 fd0Wal, _ := openIfExists(db0+"-wal", os.O_RDONLY)
 fd0Shm, _ := openIfExists(db0+"-shm", os.O_RDONLY)
 fd1, _ := openIfExists(db1, os.O_RDONLY)
 fd1Wal, _ := openIfExists(db1+"-wal", os.O_RDONLY)
 fd1Shm, _ := openIfExists(db1+"-shm", os.O_RDONLY)

 defer func() {
  if fd0 != nil {
   fd0.Close()
  }
  if fd0Wal != nil {
   fd0Wal.Close()
  }
  if fd0Shm != nil {
   fd0Shm.Close()
  }
  if fd1 != nil {
   fd1.Close()
  }
  if fd1Wal != nil {
   fd1Wal.Close()
  }
  if fd1Shm != nil {
   fd1Shm.Close()
  }
 }()

 lockFile := func(f *os.File) error {
  if f != nil {
   return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
  }
  return nil
 }

 unlockFile := func(f *os.File) error {
  if f != nil {
   return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
  }
  return nil
 }

 if err := lockFile(fd0); err != nil {
  return err
 }
 if err := lockFile(fd0Wal); err != nil {
  return err
 }
 if err := lockFile(fd0Shm); err != nil {
  return err
 }
 if err := lockFile(fd1); err != nil {
  return err
 }
 if err := lockFile(fd1Wal); err != nil {
  return err
 }
 if err := lockFile(fd1Shm); err != nil {
  return err
 }

 if fileExists(db1) {
  os.Remove(db1)
 }
 if fileExists(db1 + "-wal") {
  os.Remove(db1 + "-wal")
 }
 if fileExists(db1 + "-shm") {
  os.Remove(db1 + "-shm")
 }

 if fileExists(db0) {
  if err := os.Rename(db0, db1); err != nil {
   return err
  }
 }
 if fileExists(db0 + "-wal") {
  if err := os.Rename(db0+"-wal", db1+"-wal"); err != nil {
   return err
  }
 }
 if fileExists(db0 + "-shm") {
  if err := os.Rename(db0+"-shm", db1+"-shm"); err != nil {
   return err
  }
 }

 unlockFile(fd0)
 unlockFile(fd0Wal)
 unlockFile(fd0Shm)
 unlockFile(fd1)
 unlockFile(fd1Wal)
 unlockFile(fd1Shm)

 return nil
}

func handleRecord(w http.ResponseWriter, r *http.Request) {
    if !verifyAuth(r.Header.Get("Authorization")) {
        http.Error(w, "Authentication failed", http.StatusUnauthorized)
        return
    }

    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Failed to read body", http.StatusInternalServerError)
        return
    }

    buckets := strings.Split(r.Header.Get("RF-BUCKETS"), ",")
    for _, bucket := range buckets {
        bucket = strings.TrimSpace(bucket)
        dbFolderBucket := filepath.Join(dbFolder, bucket)
        dbPath := filepath.Join(dbFolderBucket, "relay0.sqlite3")
        dbPath1 := filepath.Join(dbFolderBucket, "relay1.sqlite3")

        stat, err := os.Stat(dbPath)
        if err == nil && stat.Size() > int64(dbMaxSizeKb*1024/2) {
            http.Error(w, "Database size limit exceeded", http.StatusInternalServerError)
            return
        }

        now := time.Now().Unix()
        if err == nil && (now-stat.ModTime().Unix()) > int64(tlS/2) {
            if err := permuteSqliteDb(dbPath, dbPath1); err != nil {
                http.Error(w, "Failed to permute database", http.StatusInternalServerError)
                return
            }
        }

        err = os.MkdirAll(dbFolderBucket, 0755)
        if err != nil {
            http.Error(w, "Failed to create database folder", http.StatusInternalServerError)
            return
        }

        db, err := sql.Open("sqlite3", dbPath)
        if err != nil {
            http.Error(w, "Failed to open database", http.StatusInternalServerError)
            return
        }
        defer db.Close()

        _, err = db.Exec("PRAGMA journal_mode=WAL")
        if err != nil {
            http.Error(w, "Failed to set journal mode", http.StatusInternalServerError)
            return
        }

        _, err = db.Exec("PRAGMA synchronous=NORMAL")
        if err != nil {
            http.Error(w, "Failed to set synchronous mode", http.StatusInternalServerError)
            return
        }

        _, err = db.Exec(`
            CREATE TABLE IF NOT EXISTS wrap_calls (
                id INTEGER PRIMARY KEY,
                content TEXT
            )
        `)
        if err != nil {
            http.Error(w, "Failed to create table", http.StatusInternalServerError)
            return
        }

        _, err = db.Exec("INSERT INTO wrap_calls (content) VALUES (?)", string(body))
        if err != nil {
            http.Error(w, "Failed to insert data", http.StatusInternalServerError)
            return
        }
    }

    w.WriteHeader(http.StatusOK)
}

func handleFirst(w http.ResponseWriter, r *http.Request) {
    if !verifyAuth(r.Header.Get("Authorization")) {
        http.Error(w, "Authentication failed", http.StatusUnauthorized)
        return
    }

    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    bucket := r.Header.Get("RF-BUCKET")
    if bucket == "" {
        http.Error(w, "Missing RF-BUCKET header", http.StatusBadRequest)
        return
    }

    dbPath := filepath.Join(dbFolder, bucket, "relay0.sqlite3")

    if _, err := os.Stat(dbPath); os.IsNotExist(err) {
        http.Error(w, "No content found", http.StatusNotFound)
        return
    }

    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        http.Error(w, "Failed to open database", http.StatusInternalServerError)
        return
    }
    defer db.Close()

    _, err = db.Exec("PRAGMA journal_mode=WAL")
    if err != nil {
        http.Error(w, "Failed to set journal mode", http.StatusInternalServerError)
        return
    }

    _, err = db.Exec("PRAGMA synchronous=NORMAL")
    if err != nil {
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
        http.Error(w, "No content found", http.StatusNotFound)
        return
    } else if err != nil {
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
    if !verifyAuth(r.Header.Get("Authorization")) {
        http.Error(w, "Authentication failed", http.StatusUnauthorized)
        return
    }

    if r.Method != http.MethodDelete {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    bucket := r.Header.Get("RF-BUCKET")
    if bucket == "" {
        http.Error(w, "Missing RF-BUCKET header", http.StatusBadRequest)
        return
    }

    id := r.URL.Query().Get("id")
    if id == "" {
        http.Error(w, "Missing id parameter", http.StatusBadRequest)
        return
    }

    dbPath := filepath.Join(dbFolder, bucket, "relay0.sqlite3")

    if _, err := os.Stat(dbPath); os.IsNotExist(err) {
        http.Error(w, "No content found", http.StatusNotFound)
        return
    }

    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        http.Error(w, "Failed to open database", http.StatusInternalServerError)
        return
    }
    defer db.Close()

    _, err = db.Exec("PRAGMA journal_mode=WAL")
    if err != nil {
        http.Error(w, "Failed to set journal mode", http.StatusInternalServerError)
        return
    }

    _, err = db.Exec("PRAGMA synchronous=NORMAL")
    if err != nil {
        http.Error(w, "Failed to set synchronous mode", http.StatusInternalServerError)
        return
    }

    result, err := db.Exec("DELETE FROM wrap_calls WHERE id = ?", id)
    if err != nil {
        http.Error(w, "Failed to delete record", http.StatusInternalServerError)
        return
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        http.Error(w, "No record found with the given id", http.StatusNotFound)
        return
    }

    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Record deleted successfully"))
}
