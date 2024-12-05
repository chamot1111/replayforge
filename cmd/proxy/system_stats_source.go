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
            now := time.Now()
            stats := map[string]interface{}{
                "timestamp": now,
                "envName": config.EnvName,
                "hostName": config.HostName,
            }

            metrics := map[string]float64{
                "memory_process":    float64(memStats.Alloc),
                "memory_host_total": float64(v.Total),
                "memory_host_free":  float64(v.Free),
                "memory_host_used":  v.UsedPercent,
                "cpu_percent":       c[0],
                "num_goroutines":    float64(runtime.NumGoroutine()),
                "gc_pause_ns":       float64(memStats.PauseNs[(memStats.NumGC+255)%256]),
            }

            for serieName, value := range metrics {
                stats["value"] = value
                stats["serieName"] = serieName
                stats["id"] = serieName + now.String()

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
                    Time:     now,
                }

                select {
                case s.EventChan <- event:
                    logger.TraceContext("source", s.ID, "Stats collected and sent for %s", serieName)
                default:
                    logger.WarnContext("source", s.ID, "Event channel full, dropping stats for %s", serieName)
                }
            }
        }
    }
}

// var Source SystemStatsSource
