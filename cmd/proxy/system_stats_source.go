package main

import (
    "encoding/json"
    "time"
    "fmt"
    "runtime"
    "github.com/chamot1111/replayforge/pkgs/logger"
    "github.com/shirou/gopsutil/v4/cpu"
    "github.com/shirou/gopsutil/v4/mem"
)

type SystemStatsSource struct {
    BaseSource
    Interval    time.Duration
    EventChan   chan<- EventSource
    stopChan    chan struct{}
}

func (s *SystemStatsSource) Init(config SourceConfig, eventChan chan<- EventSource) error {
    var sourceConfig struct {
        Params struct {
            IntervalSeconds int `json:"intervalSeconds"`
        } `json:"params"`
    }
    if err := json.Unmarshal(config.Params, &sourceConfig.Params); err != nil {
        return fmt.Errorf("failed to parse SystemStats source config params: %v", err)
    }

    s.BaseSource = config.BaseSource
    s.Interval = time.Duration(sourceConfig.Params.IntervalSeconds) * time.Second
    if s.Interval == 0 {
        s.Interval = 60 * time.Second // default to 60 seconds
    }
    s.EventChan = eventChan
    s.stopChan = make(chan struct{})
    return nil
}

func (s *SystemStatsSource) Start() error {
    go s.collectStats()
    logger.InfoContext("source", s.ID, "SystemStats source started with interval %v", s.Interval)
    return nil
}

func (s *SystemStatsSource) Stop() error {
    close(s.stopChan)
    return nil
}

func (s *SystemStatsSource) collectStats() {
    ticker := time.NewTicker(s.Interval)
    defer ticker.Stop()

    for {
        select {
        case <-s.stopChan:
            return
        case <-ticker.C:
            var memStats runtime.MemStats
            runtime.ReadMemStats(&memStats)

            v, err := mem.VirtualMemory()
            if err != nil {
                logger.ErrorContext("source", s.ID, "Failed to get virtual memory stats: %v", err)
                continue
            }

            c, err := cpu.Percent(time.Second, false)
            if err != nil {
                logger.ErrorContext("source", s.ID, "Failed to get CPU stats: %v", err)
                continue
            }
            stats := struct {
                Timestamp        time.Time `json:"timestamp"`
                MemoryProcess   float64   `json:"memoryProcess"`
                MemoryHostTotal float64   `json:"memoryHostTotal"`
                MemoryHostFree  float64   `json:"memoryHostFree"`
                MemoryHostUsed  float64   `json:"memoryHostUsedPct"`
                CpuPercent      float64   `json:"cpuPercentHost"`
                NumGoroutines   int       `json:"numGoroutines"`
                GcPauseNs      uint64    `json:"gcPauseNs"`
                EnvName        string    `json:"envName"`
                HostName       string    `json:"hostName"`
            }{
                Timestamp:       time.Now(),
                MemoryProcess:   float64(memStats.Alloc),
                MemoryHostTotal: float64(v.Total),
                MemoryHostFree:  float64(v.Free),
                MemoryHostUsed:  v.UsedPercent,
                CpuPercent:      c[0],
                NumGoroutines:   runtime.NumGoroutine(),
                GcPauseNs:       memStats.PauseNs[(memStats.NumGC+255)%256],
                EnvName:         config.EnvName,
                HostName:        config.HostName,
            }

            bodyJSON, err := json.Marshal(stats)
            if err != nil {
                logger.ErrorContext("source", s.ID, "Error marshaling stats: %v", err)
                continue
            }

            wrapCallObject := map[string]interface{}{
                "ip":      "127.0.0.1",
                "path":    fmt.Sprintf("ts_gauge_systemstats_%ds", int(s.Interval.Seconds())),
                "params":  map[string]string{},
                "headers": map[string]string{"Content-Type": "application/json"},
                "body":    string(bodyJSON),
                "method":  "POST",
            }

            jsonContent, err := json.Marshal(wrapCallObject)
            if err != nil {
                logger.ErrorContext("source", s.ID, "Error marshaling JSON: %v", err)
                continue
            }

            event := EventSource{
                SourceID: s.ID,
                Content:  string(jsonContent),
                Time:     time.Now(),
            }

            select {
            case s.EventChan <- event:
                logger.TraceContext("source", s.ID, "Stats collected and sent")
            default:
                logger.WarnContext("source", s.ID, "Event channel full, dropping stats")
            }
        }
    }
}

// var Source SystemStatsSource
