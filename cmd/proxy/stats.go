package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"runtime"
	"sync"
	"time"

	"github.com/chamot1111/replayforge/version"

	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
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
	AppBytesSent        uint64
	AppBytesRecv        uint64
	LastMessageDate    time.Time
}

var (
 AppBytesSentNodeInfo uint64
 AppBytesRecvNodeInfo uint64
)

func getStatuszInfo(includeLogs bool, sinkFilter string, sourceFilter string) map[string]interface{} {
	stats.RLock()
	defer stats.RUnlock()

	sinks := make(map[string]map[string]interface{})
	for _, sink := range config.Sinks {
		if sinkFilter != "" && sink.ID != sinkFilter {
			continue
		}

		count, lastMsg, err := getSinkStats(sink)
		if err != nil {
			sinks[sink.ID] = map[string]interface{}{
				"error": err.Error(),
			}
		} else {
			sinkInfo := map[string]interface{}{
				"id":                stats.Sinks[sink.ID].ID,
				"url":               stats.Sinks[sink.ID].URL,
				"messagesByMinute":  stats.Sinks[sink.ID].MessagesByMinute,
				"messageSinceStart": stats.Sinks[sink.ID].MessagesSinceStart,
				"lastMessageDate":   stats.Sinks[sink.ID].LastMessageDate,
				"totalEvents":       count,
				"batchCounter":      sink.batchCounter,
				"appBytesSent":      stats.Sinks[sink.ID].AppBytesSent,
				"appBytesRecv":      stats.Sinks[sink.ID].AppBytesRecv,
				"lastMessage": func(msg string) string {
					if len(msg) > 15 {
						return msg[:15] + "..."
					}
					return msg
				}(lastMsg),
			}

			if includeLogs {
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
				sinkInfo["recentLogs"] = simpleLogs
			}
			sinks[sink.ID] = sinkInfo
		}
	}

	sources := make(map[string]map[string]interface{})
	for _, source := range config.Sources {
		var sourceConfig SourceConfig
		json.Unmarshal(source, &sourceConfig)

		if sourceFilter != "" && sourceConfig.ID != sourceFilter {
			continue
		}

		sourceInfo := map[string]interface{}{
			"type":              stats.Sources[sourceConfig.ID].Type,
			"id":                stats.Sources[sourceConfig.ID].ID,
			"messagesByMinute":  stats.Sources[sourceConfig.ID].MessagesByMinute,
			"messageSinceStart": stats.Sources[sourceConfig.ID].MessagesSinceStart,
			"lastMessageDate":   stats.Sources[sourceConfig.ID].LastMessageDate,
		}

		if includeLogs {
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
			sourceInfo["recentLogs"] = simpleLogs
		}
		sources[sourceConfig.ID] = sourceInfo
	}

	return map[string]interface{}{
		"sources": sources,
		"sinks":   sinks,
		"uptime":  time.Since(stats.Started).String(),
		"version": version.Version,
	}
}

func startNodeInfoReporting() {
	logger.Info("Starting node info reporting")
	go func() {
		for {
			logger.Trace("Sending node info")
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			v, _ := mem.VirtualMemory()
			c, _ := cpu.Percent(time.Second, false)
			d, _ := disk.Usage("/")
			warnCount, errorCount := logger.GetLogStats()

			statuszInfo := getStatuszInfo(false, "", "")
			var totalBytesSent uint64
			var totalBytesRecv uint64
			for _, sink := range stats.Sinks {
				totalBytesSent += sink.AppBytesSent
				totalBytesRecv += sink.AppBytesRecv
			}
			nodeInfo := struct {
				MemoryProcess     float64                `json:"memoryProcess"`
				MemoryHostTotal   float64                `json:"memoryHostTotal"`
				MemoryHostFree    float64                `json:"memoryHostFree"`
				MemoryHostUsedPct float64                `json:"memoryHostUsedPct"`
				CpuPercentHost    float64                `json:"cpuPercentHost"`
				DiskTotal         float64                `json:"diskTotal"`
				DiskFree          float64                `json:"diskFree"`
				DiskUsedPct       float64                `json:"diskUsedPct"`
				LastUpdated       time.Time              `json:"lastUpdated"`
				WarnCount         int64                  `json:"warnCount"`
				ErrorCount        int64                  `json:"errorCount"`
				StatuszInfo       map[string]interface{} `json:"statuszInfo"`
				AppBytesSentNodeInfo      uint64                 `json:"appBytesSentNodeInfo"`
				AppBytesRecvNodeInfo      uint64                 `json:"appBytesRecvNodeInfo"`
				AppBytesSentTotal      uint64                 `json:"appBytesSentTotal"`
				AppBytesRecvTotal      uint64                 `json:"appBytesRecvTotal"`
				EnvName 		 string                 `json:"envName"`
				TsComputedName   string                 `json:"tsComputedName"`
			}{
				MemoryProcess:     float64(memStats.Alloc),
				MemoryHostTotal:   float64(v.Total),
				MemoryHostFree:    float64(v.Free),
				MemoryHostUsedPct: v.UsedPercent,
				CpuPercentHost:    c[0],
				DiskTotal:         float64(d.Total),
				DiskFree:          float64(d.Free),
				DiskUsedPct:       d.UsedPercent,
				LastUpdated:       time.Now(),
				WarnCount:         warnCount,
				ErrorCount:        errorCount,
				StatuszInfo:       statuszInfo,
				AppBytesSentNodeInfo:      AppBytesSentNodeInfo,
				AppBytesRecvNodeInfo:      AppBytesRecvNodeInfo,
				AppBytesSentTotal:      AppBytesSentNodeInfo + totalBytesSent,
				AppBytesRecvTotal:      AppBytesRecvNodeInfo + totalBytesRecv,
				EnvName: config.EnvName,
				TsComputedName: tsComputedName,
			}

			jsonData, err := json.Marshal(nodeInfo)
			if err != nil {
				logger.Error("Failed to marshal node info: %v", err)
				continue
			}
			sentUrls := make(map[string]bool)
			for _, sink := range config.Sinks {
				if sink.NotRelay || sentUrls[sink.URL] {
					logger.TraceContext("sink", sink.ID, "Skipping node info relay to %s", sink.URL)
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

				if len(sink.Buckets) == 0 {
					logger.ErrorContext("sink", sink.ID, "No buckets configured for sink")
					continue
				}
				req.Header.Set("RF-BUCKET", sink.Buckets[0])
				req.Header.Set("RF-HOSTNAME", config.HostName)

				logger.TraceContext("sink", sink.ID, "Sending node info to %s", sink.URL)
				resp, err := client.Do(req)
				if err != nil {
					logger.ErrorContext("sink", sink.ID, "Failed to send node info: %v", err)
					continue
				}
				AppBytesSentNodeInfo += uint64(len(jsonData))
				if resp.ContentLength > 0 {
					AppBytesRecvNodeInfo += uint64(resp.ContentLength)
				}
				logger.TraceContext("sink", sink.ID, "Node info response: %s", resp.Status)
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					body, _ := io.ReadAll(resp.Body)
					logger.ErrorContext("sink", sink.ID, "Bad node info response status %s: %s", resp.Status, string(body))
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
		if config.EnablePprof {
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		}
		mux.HandleFunc("/statusz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")

			sinkFilter := r.URL.Query().Get("sink")
			sourceFilter := r.URL.Query().Get("source")

			statuszInfo := getStatuszInfo(true, sinkFilter, sourceFilter)

			enc := json.NewEncoder(w)
			enc.SetIndent("", "    ")
			enc.Encode(statuszInfo)
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
