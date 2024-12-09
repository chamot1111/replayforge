package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/go-lua"

	_ "github.com/mattn/go-sqlite3"
	"tailscale.com/tsnet"

	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/chamot1111/replayforge/version"
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
	heartbeatIntervalMs = 1000
	maxDbSize           = int64(10 * 1024 * 1024)
	config              Config
	vms                 map[string]*lua.State
	lastVacuumTimes     map[string]time.Time
	sinkChannels        map[string]chan string
	tsnetServer         *tsnet.Server
	stats               Stats
	dbgScriptPath       string
	dbgFilePath         string
	tsComputedName      string
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
	logLevel := os.Getenv("RPF_LOG_LEVEL")
	if logLevel != "" {
		logger.SetLogLevel(logLevel)
	}

	startStatsCleaner()
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
}

func parseFlags() {
	flag.StringVar(&configPath, "c", "", "Path to config file")
	flag.StringVar(&dbgScriptPath, "dbg", "", "Debug mode: path to script file")
	flag.StringVar(&dbgFilePath, "dbg-file", "", "Debug mode: path to input file")
	versionFlag := flag.Bool("v", false, "Affiche la version")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("Version: %s\n", version.Version)
		os.Exit(0)
	}

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

func startSources() {
	for _, rawSource := range config.Sources {
		var sourceConfig SourceConfig
		json.Unmarshal(rawSource, &sourceConfig)

		source, ok := sources[sourceConfig.ID]
		if !ok {
			logger.FatalContext("source", sourceConfig.ID, "Source not initialized")
		}

		if err := source.Start(); err != nil {
			logger.FatalContext("source", sourceConfig.ID, "Failed to start source: %v", err)
		}
	}
}

func startSinkProcessing() {
	for _, sink := range config.Sinks {
		db, err, isSpaceError := initSetupSql(sink.DatabasePath, false, "sink", sink.ID)
		if err != nil {
			if !isSpaceError {
				logger.FatalContext("sink", sink.ID, "Error setting up SQL: %v", err)
			} else {
				logger.WarnContext("sink", sink.ID, "Space error setting up SQL: %v", err)
			}
		}
		db.Close()

		go processSinkData(sink)
		go processSinkRelay(sink)
	}
}

func processSinkData(s Sink) {
	db, err, _ := setupSql(s.DatabasePath, true, "sink", s.ID)
	if err != nil {
		if db != nil {
			db.Close()
			db = nil
		}
		logger.ErrorContext("sink", s.ID, "Failed to open database: %v", err)
	}
	insertCount := 0
	for content := range sinkChannels[s.ID] {
		insertCount++
		if db == nil {
			logger.WarnContext("sink", s.ID, "Database not ready, waiting for next cycle")
		} else {
			_, err = db.Exec("INSERT INTO sink_events (content) VALUES (?)", content)
			if err != nil {
				logger.ErrorContext("sink", s.ID, "Failed to insert into sink_events: %v", err)
			}
		}
		if insertCount >= 100 {
			insertCount = 0
			if db != nil {
				db.Close()
				db = nil
			}

			db, err, _ = setupSql(s.DatabasePath, true, "sink", s.ID)
			if err != nil {
				if db != nil {
					db.Close()
					db = nil
				}
				logger.ErrorContext("sink", s.ID, "Failed to reopen database: %v", err)
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
			logger.DebugContext("sink", s.ID, "Using backoff delay of %v\n", newDelay)
			time.Sleep(newDelay)
		} else {
			sinkBackoffDelays.Store(s.ID, initialBackoffDelay)
			logger.DebugContext("sink", s.ID, "No backoff delay of %v\n", time.Duration(heartbeatIntervalMs) * time.Millisecond)
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
