package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"runtime"

	"github.com/Shopify/go-lua"
	_ "github.com/mattn/go-sqlite3"
	"github.com/chamot1111/replayforge/internal/envparser"
	"tailscale.com/tsnet"

	"github.com/chamot1111/replayforge/pkgs/lualibs"
	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/cpu"
)
const (
	relayURLSpecialValue = ":dbg>stdout:"
	debugConfig          = `
	{
			"sources": [
					{
							"id": "dbg_source",
							"type": "repeatfile",
							"params": {
									"filePath": "${file_path}",
									"interval": 10000
							},
							"transformScript": "${script_path}",
							"hookInterval": 1000
					}
			],
			"sinks": [
					{
							"id": "dbg_sink",
							"type": "http",
							"url": ":dbg>stdout:",
							"buckets": [],
							"useTsnet": false
					}
			]
	}
`
)
var (
	sinkBackoffDelays = make(map[string]time.Duration)
	maxBackoffDelay = 300 * time.Second
	initialBackoffDelay = 100 * time.Millisecond
	defaultRateLimit = 600 // Default rate limit of 6000 messages per minute
)

type BaseSource struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	TransformScript string `json:"transformScript"`
	TargetSink      string `json:"targetSink"`
	HookInterval    int    `json:"hookInterval"`
}

type SourceConfig struct {
	BaseSource
	Params json.RawMessage `json:"params"`
}

type RateLimiter struct {
	limit         int
	count         int
	lastReset     time.Time
	mutex         sync.Mutex
}

func NewRateLimiter(limit int) *RateLimiter {
	return &RateLimiter{
		limit:     limit,
		lastReset: time.Now(),
	}
}

func (r *RateLimiter) Allow() bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	now := time.Now()
	if now.Sub(r.lastReset) >= time.Minute {
		r.count = 0
		r.lastReset = now
	}

	if r.count >= r.limit {
		return false
	}

	r.count++
	return true
}

var rateLimiters = make(map[string]*RateLimiter)
var rateLimitersMutex sync.RWMutex

type Sink struct {
	ID           string
	Type         string
	URL          string
	AuthBearer   string
	Buckets      []string
	DatabasePath string
	UseTsnet     bool `json:"useTsnet"`
	MaxMessagesPerMinute int `json:"maxMessagesPerMinute"`
}

type Config struct {
	Sources       []json.RawMessage
	Sinks         []Sink
	TsnetHostname string `json:"tsnetHostname"`
	PortStatusZ   int    `json:"portStatusZ"`
	EnvName       string `json:"envName"`
	HostName      string `json:"hostName"`
}

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

var (
	sources             map[string]Source
	configPath          string
	heartbeatIntervalMs = 100
	maxDbSize           = int64(10 * 1024 * 1024) // 10 MB
	config              Config
	vms                 map[string]*lua.State
	lastVacuumTimes     map[string]time.Time
	sinkChannels        map[string]chan string
	tsnetServer         *tsnet.Server
	stats               Stats
)

func getRateLimiter(sinkID string, maxPerMinute int) *RateLimiter {
	rateLimitersMutex.Lock()
	defer rateLimitersMutex.Unlock()

	if limiter, exists := rateLimiters[sinkID]; exists {
		return limiter
	}

	limiter := NewRateLimiter(maxPerMinute)
	rateLimiters[sinkID] = limiter
	return limiter
}

func getSinkMaxMessagesPerMinute(sink Sink) int {
 if sink.MaxMessagesPerMinute == 0 {
  return defaultRateLimit
 }
 return sink.MaxMessagesPerMinute
}

func setupSql(dbPath string, canVacuum bool) (*sql.DB, error, bool) {
	info, err := os.Stat(dbPath)
	var errDbSize error
	if err == nil && info.Size() > maxDbSize {
		errDbSize = fmt.Errorf("database size (%d bytes) exceeded maximum limit (%d bytes)", info.Size(), maxDbSize)
	}

	db, err := sql.Open("sqlite3", dbPath+"?_auto_vacuum=2&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err), false
	}

	if errDbSize != nil {
		return db, errDbSize, true
	}

	return db, nil, false
}

func initSetupSql(dbPath string, isSource bool) (*sql.DB, error, bool) {
	db, err, isSpaceError := setupSql(dbPath, true)
	if err != nil && !isSpaceError {
		if db != nil {
			db.Close()
		}
		return nil, fmt.Errorf("failed to setup database: %v", err), isSpaceError
	}

	if isSource {
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS source_events (
				id INTEGER PRIMARY KEY,
				content TEXT
			)
		`)
		if err != nil {
			return nil, fmt.Errorf("failed to create source_events table: %v", err), false
		}
	} else {
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS sink_events (
				id INTEGER PRIMARY KEY,
				content TEXT
			)
		`)
		if err != nil {
			return nil, fmt.Errorf("failed to create sink_events table: %v", err), false
		}
	}

	if isSpaceError {
		return db, fmt.Errorf("failed to setup database due to space constraints: %v", err), isSpaceError
	}
	return db, nil, false
}

func processEvent(event EventSource) {
	vm, ok := vms[event.SourceID]
	if !ok {
		logger.Warn("VM not found for source %s", event.SourceID)
		return
	}

	vm.Global("Process")
	if !vm.IsFunction(-1) {
		logger.Warn("process function not found in script for source %s", event.SourceID)
		vm.Pop(1)
		return
	}

	vm.PushString(event.Content)
	vm.PushGoFunction(func(l *lua.State) int {
		sinkID, _ := l.ToString(-2)
		emittedContent, _ := l.ToString(-1)
		if ch, ok := sinkChannels[sinkID]; ok {
			stats.Lock()
			sinkStats := stats.Sinks[sinkID]
			sinkStats.MessagesByMinute++
			sinkStats.MessagesSinceStart++
			sinkStats.LastMessageDate = time.Now()
			stats.Sinks[sinkID] = sinkStats
			stats.Unlock()
			ch <- emittedContent
		} else {
			logger.Warn("Sink channel not found for sink %s", sinkID)
		}
		return 0
	})

	if err := vm.ProtectedCall(2, 0, 0); err != nil {
		logger.Error("Failed to run process function for source %s: %v", event.SourceID, err)
	}
}

func sinkDbToRelayServer(sink Sink) error {
	if _, err := os.Stat(sink.DatabasePath); os.IsNotExist(err) {
		logger.Info("Database file does not exist: %s", sink.DatabasePath)
		return nil
	}

	sinkDB, err, _ := setupSql(sink.DatabasePath, false)
	if err != nil {
		logger.Error("Failed to open sink database: %v", err)
		return err
	}
	defer sinkDB.Close()

	// Process sink_events
	rows, err := sinkDB.Query("SELECT id, content FROM sink_events ORDER BY id ASC LIMIT 1000")
	if err != nil {
		logger.Error("Failed to query sink_events: %v", err)
		return err
	}
	defer rows.Close()

	var idsToDelete []int
	var batchContent []string
	var batchSize int

	// Create HTTP client once for the whole function
	var client *http.Client
	if sink.UseTsnet && tsnetServer != nil {
		client = tsnetServer.HTTPClient()
		client.Timeout = time.Second
	} else {
		client = &http.Client{
			Timeout: time.Second,
		}
	}

	for rows.Next() {
		var id int
		var content string
		err := rows.Scan(&id, &content)
		logger.Debug("Processing sink event: id=%d, content=%s", id, content)
		if err != nil {
			logger.Error("Failed to scan row from sink_events: %v", err)
			continue
		}

		batchContent = append(batchContent, content)
		batchSize += len(content)
		idsToDelete = append(idsToDelete, id)

		// Send batch if we hit max events or size limit
		if len(batchContent) >= 10 || batchSize >= 500*1024 {
			if err := sendBatchContent(sink, batchContent, client); err != nil {
				logger.Error("Failed to send batch content: %v", err)
				// Remove successful IDs from idsToDelete
				idsToDelete = idsToDelete[len(batchContent):]
				return err
			}
			batchContent = nil
			batchSize = 0
		}
	}

	// Send any remaining content
	if len(batchContent) > 0 {
		if err := sendBatchContent(sink, batchContent, client); err != nil {
			logger.Error("Failed to send remaining batch content: %v", err)
			// Remove successful IDs from idsToDelete
			idsToDelete = idsToDelete[len(batchContent):]
			return err
		}
	}

	if len(idsToDelete) > 0 {
		query := fmt.Sprintf("DELETE FROM sink_events WHERE id IN (%s)", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(idsToDelete)), ","), "[]"))
		_, err = sinkDB.Exec(query)
		if err != nil {
			logger.Error("Failed to delete records from sink_events: %v", err)
			return err
		}
	}

	return nil
}

func sendBatchContent(sink Sink, contents []string, client *http.Client) error {
	if sink.URL == relayURLSpecialValue {
		for _, content := range contents {
			logger.Debug("Sending content to stdout: %s", content)
		}
		return nil
	}

	maxPerMinute := getSinkMaxMessagesPerMinute(sink)
	if maxPerMinute > 0 {
		limiter := getRateLimiter(sink.ID, maxPerMinute)
		if !limiter.Allow() {
			return fmt.Errorf("maximum messages per minute exceeded for sink %s", sink.ID)
		}
	}

	batchJSON, err := json.Marshal(contents)
	if err != nil {
		return fmt.Errorf("failed to marshal batch content: %v", err)
	}

	logger.Debug("Sending batch content: %s", string(batchJSON))

	req, err := http.NewRequest("POST", sink.URL+"record-batch", bytes.NewReader(batchJSON))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sink.AuthBearer)
	req.Header.Set("RF-BUCKETS", strings.Join(sink.Buckets, ","))
	if config.EnvName != "" {
		req.Header.Set("RF-ENV-NAME", config.EnvName)
	}
	req.Header.Set("RF-HOSTNAME", config.HostName)

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

func timerHandler(sourceID string) {
	vm, ok := vms[sourceID]
	if !ok {
		return
	}

	vm.Global("TimerHandler")
	if !vm.IsFunction(-1) {
		vm.Pop(1)
		return
	}

	vm.PushGoFunction(func(l *lua.State) int {
		sinkID, _ := l.ToString(-2)
		emittedContent, _ := l.ToString(-1)
		if ch, ok := sinkChannels[sinkID]; ok {
			ch <- emittedContent
		} else {
			logger.Warn("Sink channel not found for sink %s", sinkID)
		}
		return 0
	})

	if err := vm.ProtectedCall(1, 0, 0); err != nil {
		logger.Error("Failed to run timer_handler function for source %s: %v", sourceID, err)
	}
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
    MemoryProcess      float64   `json:"memoryProcess"`
    MemoryHostTotal    float64   `json:"memoryHostTotal"`
    MemoryHostFree     float64   `json:"memoryHostFree"`
    MemoryHostUsedPct  float64   `json:"memoryHostUsedPct"`
    CpuPercentHost     float64   `json:"cpuPercentHost"`
    LastUpdated        time.Time `json:"lastUpdated"`
    WarnCount         int64       `json:"warnCount"`
    ErrorCount        int64       `json:"errorCount"`
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
     logger.Error("Failed to create node info request: %v", err)
     continue
    }

    req.Header.Set("Content-Type", "application/json")
    if sink.AuthBearer != "" {
      req.Header.Set("Authorization", "Bearer "+sink.AuthBearer)
    }

    resp, err := client.Do(req)
    if err != nil {
     logger.Error("Failed to send node info: %v", err)
     continue
    }
    resp.Body.Close()
   }

   time.Sleep(time.Minute)
  }
 }()
}

func main() {
	if tsnetServer != nil {
		go func() {
			if err := tsnetServer.Start(); err != nil {
				logger.Fatal("Failed to start tsnet server: %v", err)
			}
		}()
		defer tsnetServer.Close()
	}

	for _, rawSource := range config.Sources {
		var sourceConfig SourceConfig
		json.Unmarshal(rawSource, &sourceConfig)

		source, ok := sources[sourceConfig.ID]
		if !ok {
			logger.Fatal("Source %s not initialized", sourceConfig.ID)
		}

		if err := source.Start(); err != nil {
			logger.Fatal("Failed to start source %s: %v", sourceConfig.ID, err)
		}
	}

	for _, sink := range config.Sinks {
		db, err, isSpaceError := initSetupSql(sink.DatabasePath, false)
		if err != nil {
			if !isSpaceError {
				logger.Fatal("Error setting up SQL for sink %s: %v", sink.ID, err)
			} else {
				logger.Warn("Space error setting up SQL for sink %s: %v", sink.ID, err)
			}
		}
		db.Close()

		go func(s Sink) {
			db, err, _ := setupSql(s.DatabasePath, true)
			if err != nil {
				if(db != nil) {
					db.Close()
					db = nil
				}
				logger.Error("Failed to open database for sink %s: %v", s.ID, err)
			}
			insertCount := 0
			for content := range sinkChannels[s.ID] {
				insertCount++
				if db == nil {
					logger.Warn("Database not ready for sink %s, waiting for next cycle", s.ID)
				} else {
					_, err = db.Exec("INSERT INTO sink_events (content) VALUES (?)", content)
					if err != nil {
						logger.Error("Failed to insert into sink_events for sink %s: %v", s.ID, err)
					}
				}
				if insertCount >= 100 {
					insertCount = 0
					if db != nil {
						db.Close()
						db = nil
					}

					db, err, _ = setupSql(s.DatabasePath, true)
					if err != nil {
						if db != nil {
							db.Close()
							db = nil
						}
						logger.Error("Failed to reopen database for sink %s: %v", s.ID, err)
					}
				}
			}
			db.Close()
		}(sink)
	}

	for _, sink := range config.Sinks {
		go func(s Sink) {
			for {
				err := sinkDbToRelayServer(s)
				if err != nil {
					sinkBackoffDelays[s.ID] = time.Duration(float64(sinkBackoffDelays[s.ID]) * 2)
					if sinkBackoffDelays[s.ID] > maxBackoffDelay {
						sinkBackoffDelays[s.ID] = maxBackoffDelay
					}
					time.Sleep(sinkBackoffDelays[s.ID])
				} else {
					sinkBackoffDelays[s.ID] = initialBackoffDelay
					time.Sleep(time.Duration(heartbeatIntervalMs) * time.Millisecond)
				}
			}
		}(sink)
	}

	for _, rawSource := range config.Sources {
		var sourceConfig SourceConfig
		json.Unmarshal(rawSource, &sourceConfig)

		if sourceConfig.TransformScript != "" || sourceConfig.TargetSink != "" {
			go func(s SourceConfig) {
				for {
					timerHandler(s.ID)
					time.Sleep(time.Duration(s.HookInterval) * time.Second)
				}
			}(sourceConfig)
		}
	}

	if config.PortStatusZ > 0 {
		mux := http.NewServeMux()
		mux.HandleFunc("/statusz", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			stats.RLock()
			defer stats.RUnlock()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"sources": stats.Sources,
				"sinks":   stats.Sinks,
				"uptime":  time.Since(stats.Started).String(),
			})
		})
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte("ok"))
		})
		go http.ListenAndServe(fmt.Sprintf(":%d", config.PortStatusZ), mux)
	}

	startNodeInfoReporting()

	select {}
}
