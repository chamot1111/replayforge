package main

import (
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "time"
)

type HTTPSource struct {
    BaseSource
    ListenPort   int
    DatabasePath string
    EventChan chan<- EventSource
}

func (h *HTTPSource) Init(config SourceConfig, eventChan chan<- EventSource) error {
    var sourceConfig struct {
        Params struct {
            ListenPort int `json:"listenPort"`
        } `json:"params"`
    }
    if err := json.Unmarshal(config.Params, &sourceConfig.Params); err != nil {
        return fmt.Errorf("failed to parse HTTP source config params: %v", err)
    }
    h.BaseSource = config.BaseSource
    h.ListenPort = sourceConfig.Params.ListenPort
    h.DatabasePath = fmt.Sprintf("%s-source.sqlite3", h.ID)
    h.EventChan = eventChan
    return nil
}

func (h *HTTPSource) Start() error {
    db, err := setupSql(h.DatabasePath, true)
    if (err != nil) {
        return fmt.Errorf("error setting up SQL for source %s: %v", h.ID, err)
    }
    defer db.Close()

    mux := http.NewServeMux()
    mux.HandleFunc("/", h.handleRequest)
    server := &http.Server{
        Addr:    fmt.Sprintf(":%d", h.ListenPort),
        Handler: mux,
    }

    go func() {
        log.Printf("HTTP source listening on port %d", h.ListenPort)
        if err := server.ListenAndServe(); err != nil {
            log.Printf("HTTP server error: %v", err)
        }
    }()

    return nil
}

func (h *HTTPSource) Stop() error {
    // Implement graceful shutdown if needed
    return nil
}

func (h *HTTPSource) handleRequest(w http.ResponseWriter, r *http.Request) {
    db, err := setupSql(h.DatabasePath, false)
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
        log.Printf("Error marshaling JSON: %v", err)
        http.Error(w, fmt.Sprintf("Error: %v", err), http.StatusInternalServerError)
        return
    }

    event := EventSource{
        SourceID: h.ID,
        Content:  string(jsonContent),
        Time:     time.Now(),
    }
    select {
    case h.EventChan <- event:
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("Request received"))
    default:
        log.Printf("Event channel full, dropping event")
        w.WriteHeader(http.StatusServiceUnavailable)
        w.Write([]byte("Server busy, try again later"))
    }
}

// var Source HTTPSource
