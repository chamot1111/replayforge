package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/Shopify/go-lua"

	_ "github.com/mattn/go-sqlite3"
	"tailscale.com/tsnet"

	"github.com/chamot1111/replayforge/pkgs/logger"


)

const (
	debugConfig = `
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
							"hookInterval": "1s"
					}
			],
			"sinks": [
					{
							"id": "dbg_sink",
							"type": "http",
							"url": ":dbg>stdout:",
							"buckets": [],
							"useTsnet": false,
							"transformScript": ""
					}
			]
	}
`
)

var (
	sinkBackoffDelays   sync.Map
	maxBackoffDelay     = 300 * time.Second
	initialBackoffDelay = 100 * time.Millisecond
	defaultRateLimit    = 600
)

type RateLimiter struct {
	limit     int
	count     int
	lastReset time.Time
	mutex     sync.Mutex
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
var (
	sources             map[string]Source
	configPath          string
	heartbeatIntervalMs = 100
	maxDbSize           = int64(10 * 1024 * 1024)
	config              Config
	vms                 map[string]*lua.State
	lastVacuumTimes     map[string]time.Time
	sinkChannels        map[string]chan string
	tsnetServer         *tsnet.Server
	stats               Stats
	dbgScriptPath       string
	dbgFilePath         string
)

// init initializes configuration and sets up resources
func init() {
	initStats()
	parseFlags()
	loadConfig()
	setupSinks()
	setupSources()
}

// main starts all services and runs the application
func main() {
	startStatsCleaner()
	startTsnetServer()
	startSources()
	startSinkProcessing()
	startTimerHandlers()
	startStatusServer()
	startNodeInfoReporting()

	select {} // Block forever
}

func initStats() {
	stats = Stats{
		Sources: make(map[string]SourceStats),
		Sinks:   make(map[string]SinkStats),
		Started: time.Now(),
	}

	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel != "" {
		logger.SetLogLevel(logLevel)
	}
}

func parseFlags() {
	flag.StringVar(&configPath, "c", "", "Path to config file")
	flag.StringVar(&dbgScriptPath, "dbg", "", "Debug mode: path to script file")
	flag.StringVar(&dbgFilePath, "dbg-file", "", "Debug mode: path to input file")
	flag.Parse()

	if configPath == "" && dbgScriptPath == "" {
		logger.Fatal("Either config file path (-c) or debug script path (--dbg) must be provided")
	}

	if dbgScriptPath != "" && dbgFilePath == "" {
		logger.Fatal("In debug mode, both script path (--dbg) and file path (--dbg-file) must be provided")
	}
}



func startStatsCleaner() {
	go func() {
		for {
			time.Sleep(time.Minute)
			stats.Lock()
			for id := range stats.Sources {
				source := stats.Sources[id]
				source.MessagesByMinute = 0
				stats.Sources[id] = source
			}
			for id := range stats.Sinks {
				sink := stats.Sinks[id]
				sink.MessagesByMinute = 0
				stats.Sinks[id] = sink
			}
			stats.Unlock()
		}
	}()
}

func startTsnetServer() {
	if tsnetServer != nil {
		go func() {
			if err := tsnetServer.Start(); err != nil {
				logger.Fatal("Failed to start tsnet server: %v", err)
			}
		}()
		defer tsnetServer.Close()
	}
}

func startSources() {
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
}

func startSinkProcessing() {
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

		go processSinkData(sink)
		go processSinkRelay(sink)
	}
}

func processSinkData(s Sink) {
	db, err, _ := setupSql(s.DatabasePath, true)
	if err != nil {
		if db != nil {
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
}

func processSinkRelay(s Sink) {
	for {
		err := sinkDbToRelayServer(s)
		if err != nil {
			currentDelay, _ := sinkBackoffDelays.Load(s.ID)
			newDelay := time.Duration(float64(currentDelay.(time.Duration)) * 2)
			if newDelay > maxBackoffDelay {
				newDelay = maxBackoffDelay
			}
			sinkBackoffDelays.Store(s.ID, newDelay)
			time.Sleep(newDelay)
		} else {
			sinkBackoffDelays.Store(s.ID, initialBackoffDelay)
			time.Sleep(time.Duration(heartbeatIntervalMs) * time.Millisecond)
		}
	}
}

func startTimerHandlers() {
	for _, rawSource := range config.Sources {
		var sourceConfig SourceConfig
		json.Unmarshal(rawSource, &sourceConfig)

		if sourceConfig.TransformScript != "" || sourceConfig.TargetSink != "" {
			go func(s SourceConfig) {
				for {
					timerHandler(s.ID)
					hookInterval := s.GetHookInterval()
					if hookInterval <= 0 {
						hookInterval = time.Second
					}
					time.Sleep(hookInterval)
				}
			}(sourceConfig)
		}
	}
}

func startStatusServer() {
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
}
