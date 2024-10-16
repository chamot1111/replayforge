package main

import (
 "database/sql"
 "encoding/json"
 "flag"
 "fmt"
 "io/ioutil"
 "log"
 "net/http"
 "os"
 "path/filepath"
 "strings"
 "time"

 _ "github.com/mattn/go-sqlite3"
 "github.com/golang-jwt/jwt/v5"
)

var (
 configPath              string
 proxyDbPath             = "proxy.sqlite3"
 dbFolder                string
 heartbeatIntervalMs     = 100
 maxDbSize               = int64(100 * 1024 * 1024) // 100 MB
 relayUrl                string
 relayAuthenticationBearer string
 buckets                 []string
 serverSecret            string
 db                      *sql.DB
 lastVacuumTime          time.Time
)

func init() {
 flag.StringVar(&configPath, "c", "", "Path to config file")
 flag.StringVar(&relayUrl, "r", "", "Relay URL")
 flag.StringVar(&serverSecret, "s", "", "Server secret")
 flag.StringVar(&relayAuthenticationBearer, "a", "", "Relay authentication bearer token")
 flag.Func("b", "Bucket name (can be specified multiple times)", func(s string) error {
  buckets = append(buckets, s)
  return nil
 })
 flag.Parse()

 if configPath != "" {
  log.Printf("Config path: %s", configPath)
  configData, err := ioutil.ReadFile(configPath)
  if err != nil {
   log.Fatalf("Failed to read config file: %v", err)
  }
  var config map[string]interface{}
  if err := json.Unmarshal(configData, &config); err != nil {
   log.Fatalf("Failed to parse config JSON: %v", err)
  }

  if relayUrl == "" {
   if urlFromConfig, ok := config["relayUrl"].(string); ok {
    relayUrl = urlFromConfig
   }
  }
  if serverSecret == "" {
   if secretFromConfig, ok := config["serverSecret"].(string); ok {
    serverSecret = secretFromConfig
   }
  }
  if relayAuthenticationBearer == "" {
   if bearerFromConfig, ok := config["relayAuthenticationBearer"].(string); ok {
    relayAuthenticationBearer = bearerFromConfig
   }
  }
  if len(buckets) == 0 {
   if bucketsFromConfig, ok := config["buckets"].([]interface{}); ok {
    for _, b := range bucketsFromConfig {
     if bucket, ok := b.(string); ok {
      buckets = append(buckets, bucket)
     }
    }
   }
  }
 }

 if relayUrl == "" || serverSecret == "" || len(buckets) == 0 || relayAuthenticationBearer == "" {
  log.Fatal("relayUrl, serverSecret, relayAuthenticationBearer, and at least one bucket must be provided either via command line flags or config file")
 }

 dbFolder = filepath.Dir(proxyDbPath)

 if !strings.HasSuffix(relayUrl, "/") {
  relayUrl += "/"
 }
}

func setupSql() error {
 if db == nil {
  info, err := os.Stat(proxyDbPath)
  if err == nil && info.Size() > maxDbSize {
   tempDB, err := sql.Open("sqlite3", proxyDbPath)
   if err != nil {
    return fmt.Errorf("failed to open database: %v", err)
   }
   _, err = tempDB.Exec("VACUUM")
   tempDB.Close()
   if err != nil {
    return fmt.Errorf("failed to vacuum database: %v", err)
   }
   info, _ = os.Stat(proxyDbPath)
   if info.Size() > maxDbSize {
    return fmt.Errorf("database size exceeded maximum limit")
   }
  }

  // err is already declared in the outer scope
  db, err = sql.Open("sqlite3", proxyDbPath)
  if err != nil {
   return fmt.Errorf("failed to open database: %v", err)
  }

  _, err = db.Exec("PRAGMA journal_mode=WAL")
  if err != nil {
   return fmt.Errorf("failed to set journal mode: %v", err)
  }

  _, err = db.Exec("PRAGMA synchronous=NORMAL")
  if err != nil {
   return fmt.Errorf("failed to set synchronous mode: %v", err)
  }

  _, err = db.Exec(`
   CREATE TABLE IF NOT EXISTS wrap_calls (
    id INTEGER PRIMARY KEY,
    content TEXT
   )
  `)
  if err != nil {
   return fmt.Errorf("failed to create table: %v", err)
  }
 }
 return nil
}

func handleHttpRequest(w http.ResponseWriter, r *http.Request) {
 if err := setupSql(); err != nil {
  http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
  return
 }

 ip := r.RemoteAddr
 path := r.URL.Path
 params := r.URL.Query()
 headers := r.Header
 headers.Del("Connection")
 headers.Del("Cookie")

 fmt.Printf("%s [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), r.Method, path)

 body, err := ioutil.ReadAll(r.Body)
 if err != nil {
  http.Error(w, fmt.Sprintf("Error reading body: %v", err), http.StatusInternalServerError)
  return
 }

 wrapCallObject := map[string]interface{}{
  "ip":      ip,
  "path":    path,
  "params":  params,
  "headers": headers,
  "body":    string(body),
  "method":  r.Method,
 }

 jsonContent, err := json.Marshal(wrapCallObject)
 if err != nil {
  http.Error(w, fmt.Sprintf("Error encoding JSON: %v", err), http.StatusInternalServerError)
  return
 }

 _, err = db.Exec("INSERT INTO wrap_calls (content) VALUES (?)", string(jsonContent))
 if err != nil {
  http.Error(w, fmt.Sprintf("Error inserting into database: %v", err), http.StatusInternalServerError)
  return
 }

 w.Header().Set("Content-Type", "application/json")
 w.Write([]byte(`{"status": "success"}`))
}

func serverHeartbeat() {
 if _, err := os.Stat(proxyDbPath); os.IsNotExist(err) {
  return
 }

 tempDB, err := sql.Open("sqlite3", proxyDbPath)
 if err != nil {
  log.Printf("Failed to open database: %v", err)
  return
 }
 defer tempDB.Close()

 _, err = tempDB.Exec("PRAGMA journal_mode=WAL")
 if err != nil {
  log.Printf("Failed to set journal mode: %v", err)
  return
 }

 _, err = tempDB.Exec("PRAGMA synchronous=NORMAL")
 if err != nil {
  log.Printf("Failed to set synchronous mode: %v", err)
  return
 }

 row := tempDB.QueryRow("SELECT content FROM wrap_calls WHERE id = (SELECT MIN(id) FROM wrap_calls)")
 var content string
 err = row.Scan(&content)
 if err != nil && err != sql.ErrNoRows {
  log.Printf("Failed to query database: %v", err)
  return
 }

 currentTime := time.Now()
 if lastVacuumTime.IsZero() {
  lastVacuumTime = currentTime
 }

 if currentTime.Sub(lastVacuumTime) >= time.Hour {
  _, err = tempDB.Exec("VACUUM")
  if err != nil {
   log.Printf("Failed to vacuum database: %v", err)
  }
  lastVacuumTime = currentTime
 }

 if content != "" {
  token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
   "data": content,
  })

  tokenString, err := token.SignedString([]byte(serverSecret))
  if err != nil {
   log.Printf("Failed to create JWT: %v", err)
   return
  }

  client := &http.Client{}
  req, err := http.NewRequest("POST", relayUrl+"record", strings.NewReader(tokenString))
  if err != nil {
   log.Printf("Failed to create request: %v", err)
   return
  }

  req.Header.Set("Content-Type", "application/json")
  req.Header.Set("Authorization", "Bearer "+relayAuthenticationBearer)
  req.Header.Set("RF-BUCKETS", strings.Join(buckets, ","))

  resp, err := client.Do(req)
  if err != nil {
   log.Printf("Failed to send request: %v", err)
   return
  }
  defer resp.Body.Close()

  if resp.StatusCode < 200 || resp.StatusCode >= 300 {
   body, _ := ioutil.ReadAll(resp.Body)
   log.Printf("Failed to send data to relay server: %s", string(body))
  }

  _, err = tempDB.Exec("DELETE FROM wrap_calls WHERE id = (SELECT MIN(id) FROM wrap_calls)")
  if err != nil {
   log.Printf("Failed to delete record from database: %v", err)
  }
 }
}

func main() {
 http.HandleFunc("/", handleHttpRequest)

 go func() {
  for {
   serverHeartbeat()
   time.Sleep(time.Duration(heartbeatIntervalMs) * time.Millisecond)
  }
 }()

 log.Fatal(http.ListenAndServe(":8080", nil))
}
