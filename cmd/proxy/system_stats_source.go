package main

import (
    "encoding/json"
    "time"
    "fmt"
    "runtime"
    "github.com/chamot1111/replayforge/pkgs/logger"
    "github.com/shirou/gopsutil/v4/cpu"
    "github.com/shirou/gopsutil/v4/mem"
    "github.com/shirou/gopsutil/v4/disk"
    "github.com/shirou/gopsutil/v4/net"
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
            now := time.Now()
            stats := map[string]interface{}{
                "timestamp": now,
                "envName": config.EnvName,
                "hostName": config.HostName,
            }

            metrics := make(map[string]float64)

            var memStats runtime.MemStats
            runtime.ReadMemStats(&memStats)
            metrics["memory_process"] = float64(memStats.Alloc)
            metrics["num_goroutines"] = float64(runtime.NumGoroutine())
            metrics["gc_pause_ns"] = float64(memStats.PauseNs[(memStats.NumGC+255)%256])

            if v, err := mem.VirtualMemory(); err == nil {
                metrics["memory_host_total"] = float64(v.Total)
                metrics["memory_host_free"] = float64(v.Free)
                metrics["memory_host_used"] = v.UsedPercent
            } else {
                logger.ErrorContext("source", s.ID, "Failed to get virtual memory stats: %v", err)
            }

            if c, err := cpu.Percent(time.Second, false); err == nil {
                metrics["cpu_percent"] = c[0]
            } else {
                logger.ErrorContext("source", s.ID, "Failed to get CPU stats: %v", err)
            }

            if d, err := disk.Usage("/"); err == nil {
                metrics["disk_total"] = float64(d.Total)
                metrics["disk_free"] = float64(d.Free)
                metrics["disk_used"] = d.UsedPercent
            } else {
                logger.ErrorContext("source", s.ID, "Failed to get disk stats: %v", err)
            }

            // Get network stats
            if netStats, err := net.IOCounters(true); err == nil {
                for _, iface := range netStats {
                    prefix := "network_" + iface.Name + "_"
                    metrics[prefix+"bytes_sent"] = float64(iface.BytesSent)
                    metrics[prefix+"bytes_recv"] = float64(iface.BytesRecv)
                    metrics[prefix+"packets_sent"] = float64(iface.PacketsSent)
                    metrics[prefix+"packets_recv"] = float64(iface.PacketsRecv)
                    metrics[prefix+"errin"] = float64(iface.Errin)
                    metrics[prefix+"errout"] = float64(iface.Errout)
                    metrics[prefix+"dropin"] = float64(iface.Dropin)
                    metrics[prefix+"dropout"] = float64(iface.Dropout)
                }
            } else {
                logger.ErrorContext("source", s.ID, "Failed to get network stats: %v", err)
            }

            for serieName, value := range metrics {
                stats["value"] = value
                stats["serieName"] = serieName
                stats["id"] = serieName + fmt.Sprint(now.Unix())

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
