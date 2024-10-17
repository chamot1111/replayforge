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
 "github.com/golang-jwt/jwt/v5"
 "github.com/dop251/goja"
)

var (
 configPath              string
 proxyDbPath             = "proxy.sqlite3"
 dbFolder                string
 heartbeatIntervalMs     = 100
 maxDbSize               = int64(10 * 1024 * 1024) // 10 MB
 relayUrl                string
 relayAuthenticationBearer string
 buckets                 []string
 serverSecret            string
 lastVacuumTime          time.Time
 scriptPath              string
 vm                      *goja.Runtime
 hookIntervalSeconds     int
)

func init() {
 flag.StringVar(&configPath, "c", "", "Path to config file")
 flag.StringVar(&relayUrl, "r", "", "Relay URL")
 flag.StringVar(&serverSecret, "s", "", "Server secret")
 flag.StringVar(&relayAuthenticationBearer, "a", "", "Relay authentication bearer token")
 flag.StringVar(&scriptPath, "script", "", "Path to JavaScript script")
 flag.IntVar(&hookIntervalSeconds, "hook-interval", 60, "Interval in seconds for timer_handler")
 flag.Func("b", "Bucket name (can be specified multiple times)", func(s string) error {
  buckets = append(buckets, s)
  return nil
 })
 flag.Parse()

 if configPath != "" {
  log.Printf("Config path: %s", configPath)
  configData, err := os.ReadFile(configPath)
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
  if scriptPath == "" {
   if scriptFromConfig, ok := config["scriptPath"].(string); ok {
    scriptPath = scriptFromConfig
   }
  }
  if hookIntervalSeconds == 60 {
   if intervalFromConfig, ok := config["hookIntervalSeconds"].(float64); ok {
    hookIntervalSeconds = int(intervalFromConfig)
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

 if scriptPath != "" {
  log.Printf("Loading script from: %s", scriptPath)
  vm = goja.New()

  // Add print function to the VM
  vm.Set("print", func(call goja.FunctionCall) goja.Value {
   fmt.Println("[Script]", call.Argument(0).String())
   return goja.Undefined()
  })

  script, err := os.ReadFile(scriptPath)
  if err != nil {
   log.Fatalf("Failed to read script file: %v", err)
  }
  _, err = vm.RunString(string(script))
  if err != nil {
   log.Fatalf("Failed to load script: %v", err)
  }

  // Call the init hook if it exists
  initFn, ok := goja.AssertFunction(vm.Get("init"))
  if ok {
   _, err := initFn(goja.Undefined())
   if err != nil {
    log.Fatalf("Failed to run init hook: %v", err)
   }
  }
 }
}

func setupSql() (*sql.DB, error) {
 info, err := os.Stat(proxyDbPath)
 if err == nil && info.Size() > maxDbSize {
  log.Printf("Attempting to vacuum database...")
  tempDB, err := sql.Open("sqlite3", proxyDbPath)
  if err != nil {
   return nil, fmt.Errorf("failed to open database: %v", err)
  }
  _, err = tempDB.Exec("VACUUM")
  tempDB.Close()
  if err != nil {
   return nil, fmt.Errorf("failed to vacuum database: %v", err)
  }
  info, _ = os.Stat(proxyDbPath)
  if info.Size() > maxDbSize {
   return nil, fmt.Errorf("database size (%d bytes) exceeded maximum limit (%d bytes)", info.Size(), maxDbSize)
  }
 }

 db, err := sql.Open("sqlite3", proxyDbPath)
 if err != nil {
  return nil, fmt.Errorf("failed to open database: %v", err)
 }

 _, err = db.Exec("PRAGMA journal_mode=WAL")
 if err != nil {
  return nil, fmt.Errorf("failed to set journal mode: %v", err)
 }

 _, err = db.Exec("PRAGMA synchronous=NORMAL")
 if err != nil {
  return nil, fmt.Errorf("failed to set synchronous mode: %v", err)
 }

 return db, nil
}

func initSetupSql() (*sql.DB, error) {
 db, err := setupSql()
 if err != nil {
  return nil, fmt.Errorf("failed to setup database: %v", err)
 }

 _, err = db.Exec(`
  CREATE TABLE IF NOT EXISTS wrap_calls (
   id INTEGER PRIMARY KEY,
   content TEXT
  )
 `)
 if err != nil {
  return nil, fmt.Errorf("failed to create table: %v", err)
 }

 _, err = db.Exec(`
  CREATE TABLE IF NOT EXISTS buffered_calls (
   id INTEGER PRIMARY KEY,
   content TEXT
  )
 `)
 if err != nil {
  return nil, fmt.Errorf("failed to create buffered_calls table: %v", err)
 }

 return db, nil
}

func handleHttpRequest(w http.ResponseWriter, r *http.Request) {
 db, err := setupSql()
 if err != nil {
  log.Printf("Error setting up SQL: %v", err)
  http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
  return
 }
 defer db.Close()

 ip := r.RemoteAddr
 path := r.URL.Path
 params := r.URL.Query()
 headers := r.Header
 headers.Del("Connection")
 headers.Del("Cookie")

 fmt.Printf("%s [%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), r.Method, path)

 body, err := io.ReadAll(r.Body)
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

 if vm == nil {
  _, err = db.Exec("INSERT INTO buffered_calls (content) VALUES (?)", string(jsonContent))
 } else {
  _, err = db.Exec("INSERT INTO wrap_calls (content) VALUES (?)", string(jsonContent))
 }
 if err != nil {
  http.Error(w, fmt.Sprintf("Error inserting into database: %v", err), http.StatusInternalServerError)
  return
 }

 w.Header().Set("Content-Type", "application/json")
 w.Write([]byte(`{"status": "success"}`))
}

func serverHeartbeat() {
 if _, err := os.Stat(proxyDbPath); os.IsNotExist(err) {
  log.Printf("Database file does not exist")
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

 var idsToDelete []int

 if vm != nil {
  rows, err := tempDB.Query("SELECT id, content FROM wrap_calls ORDER BY id ASC")
  if err != nil {
   log.Printf("Failed to query database: %v", err)
   return
  }
  defer rows.Close()

  for rows.Next() {
   var id int
   var content string
   err := rows.Scan(&id, &content)
   if err != nil {
    log.Printf("Failed to scan row: %v", err)
    continue
   }

   processFn, ok := goja.AssertFunction(vm.Get("process"))
   if !ok {
    log.Printf("process function not found in script")
    continue
   }

   event := vm.ToValue(content)
   emit := vm.ToValue(func(call goja.FunctionCall) goja.Value {
    emittedContent := call.Argument(0).String()
    _, err := tempDB.Exec("INSERT INTO buffered_calls (content) VALUES (?)", emittedContent)
    if err != nil {
     log.Printf("Failed to insert into buffered_calls: %v", err)
    }
    return goja.Undefined()
   })

   _, err = processFn(goja.Undefined(), event, emit)
   if err != nil {
    log.Printf("Failed to run process function: %v", err)
    continue
   }

   idsToDelete = append(idsToDelete, id)
  }

  if len(idsToDelete) > 0 {
   query := fmt.Sprintf("DELETE FROM wrap_calls WHERE id IN (%s)", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(idsToDelete)), ","), "[]"))
   _, err = tempDB.Exec(query)
   if err != nil {
    log.Printf("Failed to delete records from wrap_calls: %v", err)
   }
  }
 }

 // Process buffered_calls
 rows, err := tempDB.Query("SELECT id, content FROM buffered_calls ORDER BY id ASC")
 if err != nil {
  log.Printf("Failed to query buffered_calls: %v", err)
  return
 }
 defer rows.Close()

 idsToDelete = []int{}

 for rows.Next() {
  var id int
  var content string
  err := rows.Scan(&id, &content)
  log.Printf("Processing buffered call: id=%d, content=%s", id, content)
  if err != nil {
   log.Printf("Failed to scan row from buffered_calls: %v", err)
   continue
  }

  if err := sendContent(content); err != nil {
   log.Printf("Failed to send content: %v", err)
  } else {
   idsToDelete = append(idsToDelete, id)
  }
 }

 if len(idsToDelete) > 0 {
  query := fmt.Sprintf("DELETE FROM buffered_calls WHERE id IN (%s)", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(idsToDelete)), ","), "[]"))
  _, err = tempDB.Exec(query)
  if err != nil {
   log.Printf("Failed to delete records from buffered_calls: %v", err)
  }
 }
}

func sendContent(content string) error {
 fmt.Printf("Debug: Sending content: %s\n", content)
 token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
  "data": content,
 })

 tokenString, err := token.SignedString([]byte(serverSecret))
 if err != nil {
  return fmt.Errorf("failed to create JWT: %v", err)
 }

 client := &http.Client{
  Timeout: time.Second,
 }
 req, err := http.NewRequest("POST", relayUrl+"record", strings.NewReader(tokenString))
 if err != nil {
  return fmt.Errorf("failed to create request: %v", err)
 }

 req.Header.Set("Content-Type", "application/json")
 req.Header.Set("Authorization", "Bearer "+relayAuthenticationBearer)
 req.Header.Set("RF-BUCKETS", strings.Join(buckets, ","))

 resp, err := client.Do(req)
 if err != nil {
  return fmt.Errorf("failed to send request: %v", err)
 }
 defer resp.Body.Close()

 if resp.StatusCode < 200 || resp.StatusCode >= 300 {
  body, _ := io.ReadAll(resp.Body)
  return fmt.Errorf("failed to send data to relay server: %s", string(body))
 }

 return nil
}

func timerHandler() {
 if vm == nil {
  return
 }

 db, err := setupSql()
 if err != nil {
  log.Printf("Error setting up SQL: %v", err)
  return
 }
 defer db.Close()

 timerHandlerFn, ok := goja.AssertFunction(vm.Get("timer_handler"))
 if !ok {
  return
 }

 emit := vm.ToValue(func(call goja.FunctionCall) goja.Value {
  emittedContent := call.Argument(0).String()
  _, err := db.Exec("INSERT INTO buffered_calls (content) VALUES (?)", emittedContent)
  if err != nil {
   log.Printf("Failed to insert into buffered_calls: %v", err)
  }
  return goja.Undefined()
 })

 _, err = timerHandlerFn(goja.Undefined(), emit)
 if err != nil {
  log.Printf("Failed to run timer_handler function: %v", err)
 }
}

func main() {
 db, err := initSetupSql()
 if err != nil {
  log.Fatalf("Error setting up SQL: %v", err)
 }
 db.Close()

 http.HandleFunc("/", handleHttpRequest)

 go func() {
  for {
   serverHeartbeat()
   time.Sleep(time.Duration(heartbeatIntervalMs) * time.Millisecond)
  }
 }()

 go func() {
  for {
   timerHandler()
   time.Sleep(time.Duration(hookIntervalSeconds) * time.Second)
  }
 }()

 log.Fatal(http.ListenAndServe(":8080", nil))
}
