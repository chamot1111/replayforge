package main

import (
    "encoding/json"
    "fmt"
    "database/sql"
    _ "github.com/lib/pq"
    "time"
    "github.com/chamot1111/replayforge/pkgs/logger"
)

type PgCall struct {
    Name string `json:"name"`
    SQL  string `json:"sql"`
}

type PgCallSource struct {
    BaseSource
    Interval    time.Duration
    Host        string
    User        string
    Password    string
    Database    string
    ConnString  string
    Calls       []PgCall
    EventChan   chan<- EventSource
    Path        string
}

func (h *PgCallSource) Init(config SourceConfig, eventChan chan<- EventSource) error {
    var sourceConfig struct {
        Params struct {
            IntervalSeconds int      `json:"intervalSeconds"`
            Host           string    `json:"host"`
            User           string    `json:"user"`
            Password       string    `json:"password"`
            Database       string    `json:"database"`
            ConnString     string    `json:"connString"`
            Calls          []PgCall  `json:"calls"`
            Path          string    `json:"path"`
        } `json:"params"`
    }
    if err := json.Unmarshal(config.Params, &sourceConfig.Params); err != nil {
        return fmt.Errorf("failed to parse PG source config params: %v", err)
    }
    h.BaseSource = config.BaseSource
    h.Interval = time.Duration(sourceConfig.Params.IntervalSeconds) * time.Second
    h.Host = sourceConfig.Params.Host
    h.User = sourceConfig.Params.User
    h.Password = sourceConfig.Params.Password
    h.Database = sourceConfig.Params.Database
    h.ConnString = sourceConfig.Params.ConnString
    h.Calls = sourceConfig.Params.Calls
    if sourceConfig.Params.Path == "" {
        sourceConfig.Params.Path = "/"
    }
    h.Path = sourceConfig.Params.Path
    h.EventChan = eventChan
    return nil
}

func (h *PgCallSource) Start() error {
    go h.pollDatabase()
    return nil
}

func (h *PgCallSource) Stop() error {
    return nil
}

func (h *PgCallSource) pollDatabase() {
    ticker := time.NewTicker(h.Interval)
    defer ticker.Stop()

    for range ticker.C {
        if err := h.executeCalls(); err != nil {
            logger.Error("Error executing PG calls: %v", err)
        }
    }
}

func (h *PgCallSource) executeCalls() error {
    connStr := h.ConnString
    if connStr == "" {
        connStr = fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
            h.Host, h.User, h.Password, h.Database)
    }

    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return fmt.Errorf("error connecting to database: %v", err)
    }
    defer db.Close()
    type QueryResult struct {
        Success bool                     `json:"success"`
        Error   string                   `json:"error,omitempty"`
        Data    []map[string]interface{} `json:"data,omitempty"`
        UpdateTime time.Time             `json:"update_time,omitempty"`
    }

    results := make(map[string]interface{})

    for _, call := range h.Calls {
        queryResult := QueryResult{Success: true}
        rows, err := db.Query(call.SQL)
        if err != nil {
            logger.Error("Error executing query %s: %v", call.Name, err)
            queryResult.Success = false
            queryResult.Error = err.Error()
            results[call.Name] = queryResult
            continue
        }

        cols, _ := rows.Columns()
        var result []map[string]interface{}

        for rows.Next() {
            values := make([]interface{}, len(cols))
            pointers := make([]interface{}, len(cols))
            for i := range values {
                pointers[i] = &values[i]
            }

            if err := rows.Scan(pointers...); err != nil {
                rows.Close()
                queryResult.Success = false
                queryResult.Error = err.Error()
                results[call.Name] = queryResult
                continue
            }

            row := make(map[string]interface{})
            for i, col := range cols {
                row[col] = values[i]
            }
            result = append(result, row)
        }
        rows.Close()

        queryResult.Data = result
        queryResult.UpdateTime = time.Now()
        results[call.Name] = queryResult
    }

    jsonContent, err := json.Marshal(results)
    if err != nil {
        return fmt.Errorf("error marshaling results: %v", err)
    }

    wrapCallObject := map[string]interface{}{
        "path":    h.Path,
        "params":  map[string]interface{}{},
        "headers": map[string]interface{}{},
        "body":    string(jsonContent),
        "method":  "POST",
    }

    wrapJsonContent, err := json.Marshal(wrapCallObject)
    if err != nil {
        logger.Error("Error marshaling JSON: %v", err)
        return err
    }

    event := EventSource{
        SourceID: h.ID,
        Content:  string(wrapJsonContent),
        Time:     time.Now(),
    }

    select {
    case h.EventChan <- event:
    default:
        logger.Warn("Event channel full, dropping event")
    }

    return nil
}

// var Source PgCallSource
