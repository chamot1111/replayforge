package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"
	"fmt"
	"github.com/chamot1111/replayforge/version"

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


func startStatusServer() {
	if config.PortStatusZ > 0 {
		mux := http.NewServeMux()
		mux.HandleFunc("/statusz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			stats.RLock()
			defer stats.RUnlock()

			// Get filter parameters
			sinkFilter := r.URL.Query().Get("sink")
			sourceFilter := r.URL.Query().Get("source")

			sinks := make(map[string]map[string]interface{})
			for _, sink := range config.Sinks {
				// Skip if sink filter specified and doesn't match
				if sinkFilter != "" && sink.ID != sinkFilter {
					continue
				}

				count, lastMsg, err := getSinkStats(sink)
				if err != nil {
					sinks[sink.ID] = map[string]interface{}{
						"error": err.Error(),
					}
				} else {
					logs := logger.GetContextHistory("sink", sink.ID)
					simpleLogs := make([]map[string]interface{}, len(logs))
					for i, log := range logs {
						levelStr := ""
						switch log.Level {
						case logger.LogLevelTrace:
							levelStr = "TRACE"
						case logger.LogLevelDebug:
							levelStr = "DEBUG"
						case logger.LogLevelInfo:
							levelStr = "INFO"
						case logger.LogLevelWarn:
							levelStr = "WARN"
						case logger.LogLevelError:
							levelStr = "ERROR"
						}
						simpleLogs[i] = map[string]interface{}{
							"timestamp": log.Timestamp,
							"message":   log.Message,
							"level":     levelStr,
						}
					}

					sinkStats := stats.Sinks[sink.ID]
					sinks[sink.ID] = map[string]interface{}{
						"id":                 sinkStats.ID,
						"url":                sinkStats.URL,
						"messagesByMinute":   sinkStats.MessagesByMinute,
						"messageSinceStart":  sinkStats.MessagesSinceStart,
						"lastMessageDate":    sinkStats.LastMessageDate,
						"totalEvents":        count,
						"batchCounter":       sink.batchCounter,
						"lastMessage": func(msg string) string {
							if len(msg) > 15 {
								return msg[:15] + "..."
							}
							return msg
						}(lastMsg),
						"recentLogs": simpleLogs,
					}
				}
			}

			sources := make(map[string]map[string]interface{})
			for _, source := range config.Sources {
				var sourceConfig SourceConfig
				json.Unmarshal(source, &sourceConfig)

				// Skip if source filter specified and doesn't match
				if sourceFilter != "" && sourceConfig.ID != sourceFilter {
					continue
				}

				logs := logger.GetContextHistory("source", sourceConfig.ID)
				simpleLogs := make([]map[string]interface{}, len(logs))
				for i, log := range logs {
					levelStr := ""
					switch log.Level {
					case logger.LogLevelTrace:
						levelStr = "TRACE"
					case logger.LogLevelDebug:
						levelStr = "DEBUG"
					case logger.LogLevelInfo:
						levelStr = "INFO"
					case logger.LogLevelWarn:
						levelStr = "WARN"
					case logger.LogLevelError:
						levelStr = "ERROR"
					}
					simpleLogs[i] = map[string]interface{}{
						"timestamp": log.Timestamp,
						"message":   log.Message,
						"level":     levelStr,
					}
				}

				sourceStats := stats.Sources[sourceConfig.ID]
				sources[sourceConfig.ID] = map[string]interface{}{
					"type":               sourceStats.Type,
					"id":                 sourceStats.ID,
					"messagesByMinute":   sourceStats.MessagesByMinute,
					"messageSinceStart":  sourceStats.MessagesSinceStart,
					"lastMessageDate":    sourceStats.LastMessageDate,
					"recentLogs":         simpleLogs,
				}
			}

			enc := json.NewEncoder(w)
			enc.SetIndent("", "    ")
			enc.Encode(map[string]interface{}{
				"sources": sources,
				"sinks":   sinks,
				"uptime":  time.Since(stats.Started).String(),
				"version": version.Version,
			})
		})
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("ok"))
		})

		if config.UseTsnetStatusZ && tsnetServer != nil {
			ln, err := tsnetServer.Listen("tcp", fmt.Sprintf(":%d", config.PortStatusZ))
			if err != nil {
				logger.Fatal("Failed to create tsnet listener: %v", err)
			}
			go http.Serve(ln, mux)
		} else {
			go http.ListenAndServe(fmt.Sprintf(":%d", config.PortStatusZ), mux)
		}
	}
}
