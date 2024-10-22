package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Shopify/go-lua"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vjeantet/grok"
	"tailscale.com/tsnet"
)

const (
 relayURLSpecialValue = ":dbg>stdout:"
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

type Sink struct {
	ID           string
	Type         string
	URL          string
	AuthBearer   string
	Buckets      []string
	DatabasePath string
	UseTsnet     bool `json:"useTsnet"`
}

type Config struct {
	Sources []json.RawMessage
	Sinks   []Sink
	TsnetHostname string `json:"tsnetHostname"`
}

var (
	sources            map[string]Source
	configPath         string
	heartbeatIntervalMs = 100
	maxDbSize           = int64(10 * 1024 * 1024) // 10 MB
	config              Config
	vms                 map[string]*lua.State
	lastVacuumTimes     map[string]time.Time
	sinkChannels        map[string]chan string
	grokParser          *grok.Grok
	tsnetServer         *tsnet.Server
)
func init() {
	flag.StringVar(&configPath, "c", "", "Path to config file")
	flag.Parse()

	if configPath == "" {
		log.Fatal("Config file path must be provided using -c flag")
	}

	log.Printf("Config path: %s", configPath)
	configData, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	if err := json.Unmarshal(configData, &config); err != nil {
		log.Fatalf("Failed to parse config JSON: %v", err)
	}

	vms = make(map[string]*lua.State)
	lastVacuumTimes = make(map[string]time.Time)
	sources = make(map[string]Source)
	sinkChannels = make(map[string]chan string)

	grokParser, err = grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true})
	if err != nil {
		log.Fatalf("Failed to initialize grok parser: %v", err)
	}

	for i := range config.Sinks {
		sink := &config.Sinks[i]
		if sink.DatabasePath == "" {
			sink.DatabasePath = fmt.Sprintf("%s-sink.sqlite3", sink.ID)
		}

		if sink.URL != relayURLSpecialValue && !strings.HasSuffix(sink.URL, "/") {
			sink.URL += "/"
		}
		sinkChannels[sink.ID] = make(chan string, 100)
		db, err := initSetupSql(sink.DatabasePath, false)
		if err != nil {
			log.Fatalf("Error setting up SQL for sink %s: %v", sink.ID, err)
		}
		db.Close()
	}

	if config.TsnetHostname != "" {
		tsnetServer = &tsnet.Server{Hostname: config.TsnetHostname}
	}

	for _, rawSource := range config.Sources {
		var sourceConfig SourceConfig
		if err := json.Unmarshal(rawSource, &sourceConfig); err != nil {
			log.Fatalf("Failed to parse source JSON: %v", err)
		}

		var source Source
		switch sourceConfig.Type {
			case "http":
				source = &HTTPSource{}
			case "logfile":
				source = &LogFileSource{}
			case "repeatfile":
				source = &RepeatFileSource{}
		default:
			log.Fatalf("Unsupported source type: %s", sourceConfig.Type)
		}

		eventChan := make(chan EventSource)
		if err := source.Init(sourceConfig, eventChan); err != nil {
			log.Fatalf("Failed to initialize source %s: %v", sourceConfig.ID, err)
		}

		sources[sourceConfig.ID] = source

		go func(s Source, ch <-chan EventSource) {
			for event := range ch {
				processEvent(event)
			}
		}(source, eventChan)

		vm := lua.NewState()
		lua.OpenLibraries(vm)

		vm.Register("print", func(l *lua.State) int {
			str, _ := l.ToString(-1)
			fmt.Println("[Script]", str)
			return 0
		})

		vm.Register("grok_parse", func(l *lua.State) int {
			pattern, _ := l.ToString(-2)
			text, _ := l.ToString(-1)
			values, err := grokParser.Parse(pattern, text)
			if err != nil {
				l.PushNil()
				l.PushString(err.Error())
				return 2
			}
			l.NewTable()
			for k, v := range values {
				l.PushString(k)
				l.PushString(v)
				l.SetTable(-3)
			}
			return 1
		})

		if sourceConfig.TransformScript != "" {
			script, err := os.ReadFile(sourceConfig.TransformScript)
			if err != nil {
				log.Fatalf("Failed to read script file for source %s: %v", sourceConfig.ID, err)
			}
			if err := lua.DoString(vm, string(script)); err != nil {
				log.Fatalf("Failed to load script for source %s: %v", sourceConfig.ID, err)
			}
		} else if sourceConfig.TargetSink != "" {
			defaultScript := fmt.Sprintf(`
				function process(event, emit)
					emit("%s", event)
				end
			`, sourceConfig.TargetSink)
			if err := lua.DoString(vm, defaultScript); err != nil {
				log.Fatalf("Failed to load default script for source %s: %v", sourceConfig.ID, err)
			}
		} else {
			log.Fatalf("Either TransformScript or TargetSink must be provided for source %s", sourceConfig.ID)
		}

		vm.Global("init")
		if vm.IsFunction(-1) {
			if err := vm.ProtectedCall(0, 0, 0); err != nil {
				log.Fatalf("Failed to run init hook for source %s: %v", sourceConfig.ID, err)
			}
		}
		vm.Pop(1)

		vms[sourceConfig.ID] = vm
	}
}

func setupSql(dbPath string, canVacuum bool) (*sql.DB, error) {
	info, err := os.Stat(dbPath)
	if err == nil && info.Size() > maxDbSize {
		if canVacuum {
			log.Printf("Attempting to vacuum database: %s", dbPath)
			tempDB, err := sql.Open("sqlite3", dbPath)
			if err != nil {
				return nil, fmt.Errorf("failed to open database: %v", err)
			}
			_, err = tempDB.Exec("VACUUM")
			tempDB.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to vacuum database: %v", err)
			}
			info, _ = os.Stat(dbPath)
		}
		if info.Size() > maxDbSize {
			return nil, fmt.Errorf("database size (%d bytes) exceeded maximum limit (%d bytes)", info.Size(), maxDbSize)
		}
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %v", err)
	}

	_, err = db.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %v", err)
	}

	return db, nil
}

func initSetupSql(dbPath string, isSource bool) (*sql.DB, error) {
	db, err := setupSql(dbPath, true)
	if err != nil {
		return nil, fmt.Errorf("failed to setup database: %v", err)
	}

	if isSource {
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS source_events (
				id INTEGER PRIMARY KEY,
				content TEXT
			)
		`)
		if err != nil {
			return nil, fmt.Errorf("failed to create source_events table: %v", err)
		}
	} else {
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS sink_events (
				id INTEGER PRIMARY KEY,
				content TEXT
			)
		`)
		if err != nil {
			return nil, fmt.Errorf("failed to create sink_events table: %v", err)
		}
	}

	return db, nil
}

func processEvent(event EventSource) {
	vm, ok := vms[event.SourceID]
	if !ok {
		log.Printf("VM not found for source %s", event.SourceID)
		return
	}

	vm.Global("process")
	if !vm.IsFunction(-1) {
		log.Printf("process function not found in script for source %s", event.SourceID)
		vm.Pop(1)
		return
	}

	vm.PushString(event.Content)
	vm.PushGoFunction(func(l *lua.State) int {
		sinkID, _ := l.ToString(-2)
		emittedContent, _ := l.ToString(-1)
		if ch, ok := sinkChannels[sinkID]; ok {
			ch <- emittedContent
		} else {
			log.Printf("Sink channel not found for sink %s", sinkID)
		}
		return 0
	})

	if err := vm.ProtectedCall(2, 0, 0); err != nil {
		log.Printf("Failed to run process function for source %s: %v", event.SourceID, err)
	}
}

func insertIntoSinkEvents(sinkID, content string) error {
	for _, sink := range config.Sinks {
		if sink.ID == sinkID {
			db, err := setupSql(sink.DatabasePath, false)
			if err != nil {
				return fmt.Errorf("failed to setup SQL for sink %s: %v", sinkID, err)
			}
			defer db.Close()

			_, err = db.Exec("INSERT INTO sink_events (content) VALUES (?)", content)
			if err != nil {
				return fmt.Errorf("failed to insert into sink_events: %v", err)
			}
			return nil
		}
	}
	return fmt.Errorf("sink not found: %s", sinkID)
}

func sinkDbToRelayServer(sink Sink) {
	if _, err := os.Stat(sink.DatabasePath); os.IsNotExist(err) {
		log.Printf("Database file does not exist: %s", sink.DatabasePath)
		return
	}

	sinkDB, err := sql.Open("sqlite3", sink.DatabasePath)
	if err != nil {
		log.Printf("Failed to open sink database: %v", err)
		return
	}
	defer sinkDB.Close()

	_, err = sinkDB.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		log.Printf("Failed to set journal mode: %v", err)
		return
	}

	_, err = sinkDB.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		log.Printf("Failed to set synchronous mode: %v", err)
		return
	}

	currentTime := time.Now()
	if lastVacuumTime, ok := lastVacuumTimes[sink.ID]; !ok || currentTime.Sub(lastVacuumTime) >= time.Hour {
		_, err = sinkDB.Exec("VACUUM")
		if err != nil {
			log.Printf("Failed to vacuum database: %v", err)
		}
		lastVacuumTimes[sink.ID] = currentTime
	}

	// Process sink_events
	rows, err := sinkDB.Query("SELECT id, content FROM sink_events ORDER BY id ASC")
	if err != nil {
		log.Printf("Failed to query sink_events: %v", err)
		return
	}
	defer rows.Close()

	var idsToDelete []int

	for rows.Next() {
		var id int
		var content string
		err := rows.Scan(&id, &content)
		log.Printf("Processing sink event: id=%d, content=%s", id, content)
		if err != nil {
			log.Printf("Failed to scan row from sink_events: %v", err)
			continue
		}

		if err := sendContent(sink, content); err != nil {
			log.Printf("Failed to send content: %v", err)
		} else {
			idsToDelete = append(idsToDelete, id)
		}
	}

	if len(idsToDelete) > 0 {
		query := fmt.Sprintf("DELETE FROM sink_events WHERE id IN (%s)", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(idsToDelete)), ","), "[]"))
		_, err = sinkDB.Exec(query)
		if err != nil {
			log.Printf("Failed to delete records from sink_events: %v", err)
		}
	}
}

func sendContent(sink Sink, content string) error {
	if sink.URL == relayURLSpecialValue {
		fmt.Printf("Debug: Sending content to stdout: %s\n", content)
		return nil
	}

	fmt.Printf("Debug: Sending content: %s\n", content)

	var client *http.Client
	var req *http.Request
	var err error

	if sink.UseTsnet && tsnetServer != nil {
		client = tsnetServer.HTTPClient()
		client.Timeout = time.Second
	} else {
		client = &http.Client{
			Timeout: time.Second,
		}
	}

	req, err = http.NewRequest("POST", sink.URL+"record", strings.NewReader(content))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+sink.AuthBearer)
	req.Header.Set("RF-BUCKETS", strings.Join(sink.Buckets, ","))

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

	vm.Global("timer_handler")
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
			log.Printf("Sink channel not found for sink %s", sinkID)
		}
		return 0
	})

	if err := vm.ProtectedCall(1, 0, 0); err != nil {
		log.Printf("Failed to run timer_handler function for source %s: %v", sourceID, err)
	}
}

func main() {
	if tsnetServer != nil {
		go func() {
			if err := tsnetServer.Start(); err != nil {
				log.Fatalf("Failed to start tsnet server: %v", err)
			}
		}()
		defer tsnetServer.Close()
	}

	for _, rawSource := range config.Sources {
		var sourceConfig SourceConfig
		json.Unmarshal(rawSource, &sourceConfig)

		source, ok := sources[sourceConfig.ID]
		if !ok {
			log.Fatalf("Source %s not initialized", sourceConfig.ID)
		}

		if err := source.Start(); err != nil {
			log.Fatalf("Failed to start source %s: %v", sourceConfig.ID, err)
		}
	}

	for _, sink := range config.Sinks {
		db, err := initSetupSql(sink.DatabasePath, false)
		if err != nil {
			log.Fatalf("Error setting up SQL for sink %s: %v", sink.ID, err)
		}
		db.Close()

		go func(s Sink) {
			for content := range sinkChannels[s.ID] {
				if err := insertIntoSinkEvents(s.ID, content); err != nil {
					log.Printf("Failed to insert into sink_events for sink %s: %v", s.ID, err)
				}
			}
		}(sink)
	}

	for _, sink := range config.Sinks {
		go func(s Sink) {
			for {
				sinkDbToRelayServer(s)
				time.Sleep(time.Duration(heartbeatIntervalMs) * time.Millisecond)
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

	select {}
}
