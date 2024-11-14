package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

type Stats struct {
	sync.RWMutex
	Sources map[string]SourceStats
	Sinks   map[string]SinkStats
	Started time.Time
}

type SourceStats struct {
	Type               string
	ID                 string
	MessagesByMinute   int
	MessagesSinceStart int64
	LastMessageDate    time.Time
}

type SinkStats struct {
	ID                 string
	URL                string
	MessagesByMinute   int
	MessagesSinceStart int64
	LastMessageDate    time.Time
}


func startNodeInfoReporting() {
	go func() {
		for {
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			v, _ := mem.VirtualMemory()
			c, _ := cpu.Percent(time.Second, false)
			warnCount, errorCount := logger.GetLogStats()
			nodeInfo := struct {
				MemoryProcess     float64   `json:"memoryProcess"`
				MemoryHostTotal   float64   `json:"memoryHostTotal"`
				MemoryHostFree    float64   `json:"memoryHostFree"`
				MemoryHostUsedPct float64   `json:"memoryHostUsedPct"`
				CpuPercentHost    float64   `json:"cpuPercentHost"`
				LastUpdated       time.Time `json:"lastUpdated"`
				WarnCount         int64     `json:"warnCount"`
				ErrorCount        int64     `json:"errorCount"`
			}{
				MemoryProcess:     float64(memStats.Alloc),
				MemoryHostTotal:   float64(v.Total),
				MemoryHostFree:    float64(v.Free),
				MemoryHostUsedPct: v.UsedPercent,
				CpuPercentHost:    c[0],
				LastUpdated:       time.Now(),
				WarnCount:         warnCount,
				ErrorCount:        errorCount,
			}

			jsonData, err := json.Marshal(nodeInfo)
			if err != nil {
				logger.Error("Failed to marshal node info: %v", err)
				continue
			}
			// Keep track of URLs we've already sent to
			sentUrls := make(map[string]bool)

			for _, sink := range config.Sinks {
				// Skip if we've already sent to this URL
				if sentUrls[sink.URL] {
					continue
				}
				sentUrls[sink.URL] = true

				var client *http.Client
				if sink.UseTsnet && tsnetServer != nil {
					client = tsnetServer.HTTPClient()
				} else {
					client = &http.Client{
						Timeout: time.Second,
					}
				}

				req, err := http.NewRequest("POST", sink.URL+"node-info", bytes.NewBuffer(jsonData))
				if err != nil {
					logger.ErrorContext("sink", sink.ID, "Failed to create node info request: %v", err)
					continue
				}

				req.Header.Set("Content-Type", "application/json")
				if sink.AuthBearer != "" {
					req.Header.Set("Authorization", "Bearer "+sink.AuthBearer)
				}

				resp, err := client.Do(req)
				if err != nil {
					logger.ErrorContext("sink", sink.ID, "Failed to send node info: %v", err)
					continue
				}
				resp.Body.Close()
			}

			time.Sleep(time.Minute)
		}
	}()
}
