package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/chamot1111/replayforge/pkgs/lualibs"
)

const (
	relayURLSpecialValue    = ":dbg>stdout:"
	maxPayloadBytes         = 5 * 1024 * 1024
	batchGoalBytes          = 4.25 * 1024 * 1024
	batchMaxEvents          = 1000
	batchDefaultTimeoutSecs = 5
)

type Sink struct {
	ID                   string
	Type                 string
	URL                  string
	AuthBearer           string
	Buckets              []string
	DatabasePath         string
	UseTsnet             bool       `json:"useTsnet"`
	MaxMessagesPerMinute int        `json:"maxMessagesPerMinute"`
	TransformScript      string     `json:"transformScript"`
	Config               SinkConfig `json:"config"`
	NotRelay             bool       `json:"notRelay"`
	vm                   *SinkVM
	lastBatchTime        time.Time
	batchCounter         int          // Added field to track batch count
	httpClient           *http.Client // Added field for reusable client
}

type LuaVM struct {
	vm       *lua.State
	useCount int
	maxUses  int
	mutex    sync.Mutex
}

type SinkVM struct {
	currentVM *LuaVM
	script    string
	mutex     sync.Mutex
	id        string
}

func NewSinkVM(script string, sinkID string) *SinkVM {
	return &SinkVM{
		script: script,
		id:     sinkID,
	}
}

func sinkDbToRelayServer(sink Sink) error {
	if _, err := os.Stat(sink.DatabasePath); os.IsNotExist(err) {
		logger.InfoContext("sink", sink.ID, "Database file does not exist: %s", sink.DatabasePath)
		return nil
	}

	sinkDB, err, isErrDbSize := setupSql(sink.DatabasePath, false, "sink", sink.ID)
	if err != nil && !isErrDbSize {
		logger.ErrorContext("sink", sink.ID, "Failed to open sink database: %v", err)
		return err
	}
	defer sinkDB.Close()

	maxEvents := sink.GetBatchMaxEvents()
	goalBytes := sink.GetBatchGoalBytes()

	rows, err := sinkDB.Query(fmt.Sprintf("SELECT id, content FROM sink_events ORDER BY id ASC LIMIT %d", maxEvents))
	if err != nil {
		logger.ErrorContext("sink", sink.ID, "Failed to query sink_events: %v", err)
		return err
	}
	defer rows.Close()

	var idsToDelete []int
	var batchContent []string
	var batchSize int
	var batchSent bool

	maxPayload := sink.GetMaxPayloadBytes()

	for rows.Next() {
		var id int
		var content string
		err := rows.Scan(&id, &content)
		logger.DebugContext("sink", sink.ID, "Processing sink event: id=%d, content=%s", id, content)
		if err != nil {
			logger.ErrorContext("sink", sink.ID, "Failed to scan row from sink_events: %v", err)
			continue
		}

		contentBytes := len(content)
		if contentBytes+batchSize > maxPayload {
			logger.WarnContext("sink", sink.ID, "Content size (%d bytes) would exceed max payload size, skipping", contentBytes)
			continue
		}

		batchContent = append(batchContent, content)
		batchSize += contentBytes
		idsToDelete = append(idsToDelete, id)

		shouldSend := false
		if batchSize >= goalBytes {
			shouldSend = true
		} else if len(batchContent) >= maxEvents {
			shouldSend = true
		}

		if shouldSend {
			sink.batchCounter++
			if err := sendBatchContent(&sink, batchContent, sink.httpClient); err != nil {
				logger.ErrorContext("sink", sink.ID, "Failed to send batch content (batch #%d): %v", sink.batchCounter, err)
				idsToDelete = idsToDelete[len(batchContent):]
				return err
			}
			batchContent = nil
			batchSize = 0
			batchSent = true
		} else {
			logger.TraceContext("sink", sink.ID, "Not sending batch yet (batch #%d): %d events, %d total size, waited %v/%d seconds (last batch time: %v)",
				sink.batchCounter+1, len(batchContent), batchSize, time.Since(sink.lastBatchTime),
				sink.GetBatchTimeoutSecs(), sink.lastBatchTime)
		}
	}

	// Send any remaining messages that didn't reach goal size
	if len(batchContent) > 0 && time.Since(sink.lastBatchTime) >= time.Duration(sink.GetBatchTimeoutSecs())*time.Second {
		sink.batchCounter++
		if err := sendBatchContent(&sink, batchContent, sink.httpClient); err != nil {
			logger.ErrorContext("sink", sink.ID, "Failed to send final batch content (batch #%d): %v", sink.batchCounter, err)
			idsToDelete = idsToDelete[len(batchContent):]
			return err
		}
		batchSent = true
	} else if len(batchContent) > 0 {
		// If we don't send remaining messages, remove their IDs from deletion list
		idsToDelete = idsToDelete[:len(idsToDelete)-len(batchContent)]
	}

	if len(idsToDelete) > 0 {
		query := fmt.Sprintf("DELETE FROM sink_events WHERE id IN (%s)", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(idsToDelete)), ","), "[]"))
		_, err = sinkDB.Exec(query)
		if err != nil {
			logger.ErrorContext("sink", sink.ID, "Failed to delete records from sink_events: %v", err)
			return err
		}
	}

	if batchSent {
		sink.lastBatchTime = time.Now()
	}
	return nil
}

func sendBatchContent(sink *Sink, contents []string, client *http.Client) error {
	if sink.URL == relayURLSpecialValue {
		for _, content := range contents {
			logger.DebugContext("sink", sink.ID, "Sending content to stdout (batch #%d): %s", sink.batchCounter, content)
		}
		return nil
	}

	maxPerMinute := getSinkMaxMessagesPerMinute(*sink)
	if maxPerMinute > 0 {
		limiter := getRateLimiter(sink.ID, maxPerMinute)
		if !limiter.Allow() {
			return fmt.Errorf("maximum messages per minute exceeded for sink %s", sink.ID)
		}
	}

	result, err := transformSinkContent(sink, contents)
	if err != nil {
		return fmt.Errorf("failed to transform contents: %v", err)
	}

	batchJSON, err := json.Marshal(result.Messages)
	if err != nil {
		return fmt.Errorf("failed to marshal batch content: %v", err)
	}

	logger.DebugContext("sink", sink.ID, "Sending batch content #%d: %s", sink.batchCounter, string(batchJSON))

	url := sink.URL
	if result.Request.Path != "" {
		url = strings.TrimRight(url, "/") + "/" + strings.TrimLeft(result.Request.Path, "/")
	} else {
		url = sink.URL + "record-batch"
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(batchJSON))
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

	// Add custom headers from transform script
	for k, v := range result.Request.Headers {
		req.Header.Set(k, v)
	}

	bytesSent := uint64(len(batchJSON))

	resp, err := client.Do(req)
	if err != nil {
		// Recreate HTTP client on error
		if sink.UseTsnet && tsnetServer != nil {
			sink.httpClient = tsnetServer.HTTPClient()
		} else {
			sink.httpClient = &http.Client{}
		}
		sink.httpClient.Timeout = time.Duration(sink.GetBatchTimeoutSecs()) * time.Second
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send data to relay server: %s", string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	bytesRecv := uint64(len(body))

	stats.Lock()
	s := stats.Sinks[sink.ID]
	s.AppBytesSent += bytesSent
	s.AppBytesRecv += bytesRecv
	stats.Sinks[sink.ID] = s
	stats.Unlock()

	logger.DebugContext("sink", sink.ID, "Batch #%d response: status=%d, body=%s", sink.batchCounter, resp.StatusCode, string(body))

	return nil
}

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

func (svm *SinkVM) getVM() (*LuaVM, error) {
	svm.mutex.Lock()
	defer svm.mutex.Unlock()

	if svm.currentVM == nil || svm.currentVM.useCount >= svm.currentVM.maxUses {
		if svm.currentVM != nil {
			// Note: Lua will automatically garbage collect
			svm.currentVM = nil
		}

		vm := lua.NewState()
		lua.OpenLibraries(vm)
		lualibs.RegisterLuaLibs(vm)

		// Add logger functions
		vm.PushGoFunction(func(l *lua.State) int {
			msg, _ := l.ToString(-1)
			logger.TraceContext("sink", svm.id, msg)
			return 0
		})
		vm.SetGlobal("trace")

		vm.PushGoFunction(func(l *lua.State) int {
			msg, _ := l.ToString(-1)
			logger.DebugContext("sink", svm.id, msg)
			return 0
		})
		vm.SetGlobal("debug")

		vm.PushGoFunction(func(l *lua.State) int {
			msg, _ := l.ToString(-1)
			logger.InfoContext("sink", svm.id, msg)
			return 0
		})
		vm.SetGlobal("info")

		vm.PushGoFunction(func(l *lua.State) int {
			msg, _ := l.ToString(-1)
			logger.WarnContext("sink", svm.id, msg)
			return 0
		})
		vm.SetGlobal("warn")

		vm.PushGoFunction(func(l *lua.State) int {
			msg, _ := l.ToString(-1)
			logger.ErrorContext("sink", svm.id, msg)
			return 0
		})
		vm.SetGlobal("error")

		if err := lua.DoString(vm, svm.script); err != nil {
			// No need to explicitly close Lua VM as it will be garbage collected
			return nil, fmt.Errorf("failed to load script: %v", err)
		}

		svm.currentVM = &LuaVM{
			vm:       vm,
			useCount: 0,
			maxUses:  100,
		}
	}

	return svm.currentVM, nil
}

func (lvm *LuaVM) use() {
	lvm.mutex.Lock()
	defer lvm.mutex.Unlock()
	lvm.useCount++
}

func (s *Sink) GetMaxPayloadBytes() int {
	if s.Config.MaxPayloadBytes > 0 {
		return s.Config.MaxPayloadBytes
	}
	return maxPayloadBytes
}

func (s *Sink) GetBatchGoalBytes() int {
	if s.Config.BatchGoalBytes > 0 {
		return s.Config.BatchGoalBytes
	}
	return batchGoalBytes
}

func (s *Sink) GetBatchMaxEvents() int {
	if s.Config.BatchMaxEvents > 0 {
		return s.Config.BatchMaxEvents
	}
	return batchMaxEvents
}

func (s *Sink) GetBatchTimeoutSecs() int {
	if s.Config.BatchTimeoutSecs > 0 {
		return s.Config.BatchTimeoutSecs
	}
	return batchDefaultTimeoutSecs
}

func transformSinkContent(sink *Sink, contents []string) (*SinkTransformResult, error) {
	if sink.TransformScript == "" {
		return &SinkTransformResult{Messages: contents}, nil
	}

	luaVM, err := sink.vm.getVM()
	if err != nil {
		return nil, fmt.Errorf("failed to get Lua VM: %v", err)
	}

	luaVM.use()
	vm := luaVM.vm

	result := &SinkTransformResult{
		Request: struct {
			Path    string            `json:"path"`
			Headers map[string]string `json:"headers"`
		}{
			Headers: make(map[string]string),
		},
	}

	vm.Global("TransformBatch")
	if !vm.IsFunction(-1) {
		return nil, fmt.Errorf("TransformBatch function not found in sink script")
	}

	// Push messages table
	vm.NewTable()
	for i, content := range contents {
		vm.PushInteger(i + 1)
		vm.PushString(content)
		vm.SetTable(-3)
	}

	// Push request table
	vm.NewTable()
	vm.PushString("path")
	vm.PushString("")
	vm.SetTable(-3)
	vm.PushString("headers")
	vm.NewTable()
	vm.SetTable(-3)

	if err := vm.ProtectedCall(2, 2, 0); err != nil {
		return nil, fmt.Errorf("failed to run TransformBatch: %v", err)
	}
	// Parse request table
	if vm.IsTable(-1) {
		// Get headers
		vm.PushString("headers")
		vm.RawGet(-2)
		if vm.IsTable(-1) {
			vm.PushNil()
			for vm.Next(-2) {
				if key, ok := vm.ToString(-2); ok {
					if value, ok := vm.ToString(-1); ok {
						result.Request.Headers[key] = value
					}
				}
				vm.Pop(1)
			}
		}
		vm.Pop(1)

		// Get path
		vm.PushString("path")
		vm.RawGet(-2)
		if path, ok := vm.ToString(-1); ok {
			result.Request.Path = path
		}
		vm.Pop(1)
	}
	vm.Pop(1)

	// Parse messages table
	if !vm.IsTable(-1) {
		return nil, fmt.Errorf("TransformBatch must return messages table and request table")
	}

	var transformedContents []string
	vm.PushNil()
	for vm.Next(-2) {
		if str, ok := vm.ToString(-1); ok {
			transformedContents = append(transformedContents, str)
		}
		vm.Pop(1)
	}
	result.Messages = transformedContents

	vm.Pop(1)

	return result, nil
}

type SinkTransformResult struct {
	Messages []string
	Request  struct {
		Path    string            `json:"path"`
		Headers map[string]string `json:"headers"`
	}
}

func setupSinks() {
	for i := range config.Sinks {
		sink := &config.Sinks[i]
		if sink.DatabasePath == "" {
			sink.DatabasePath = fmt.Sprintf("%s-sink.sqlite3", sink.ID)
		}

		if sink.URL != relayURLSpecialValue && !strings.HasSuffix(sink.URL, "/") {
			sink.URL += "/"
		}

		if sink.TransformScript != "" {
			script, err := os.ReadFile(sink.TransformScript)
			if err != nil {
				logger.FatalContext("sink", sink.ID, "Failed to read transform script: %v", err)
			}
			sink.vm = NewSinkVM(string(script), sink.ID)
		}

		if sink.UseTsnet && tsnetServer != nil {
			sink.httpClient = tsnetServer.HTTPClient()
		} else {
			sink.httpClient = &http.Client{}
		}
		sink.httpClient.Timeout = time.Duration(sink.GetBatchTimeoutSecs()) * time.Second

		sinkChannels[sink.ID] = make(chan string, 1000)
		db, err, isSpaceError := initSetupSql(sink.DatabasePath, false, "sink", sink.ID)
		if err != nil {
			if !isSpaceError {
				logger.FatalContext("sink", sink.ID, "Error setting up SQL: %v", err)
			} else {
				logger.WarnContext("sink", sink.ID, "Space error setting up SQL: %v", err)
			}
		}
		db.Close()

		stats.Lock()
		stats.Sinks[sink.ID] = SinkStats{
			ID:  sink.ID,
			URL: sink.URL,
		}
		stats.Unlock()
	}

	for i := range config.Sinks {
		sinkBackoffDelays.Store(config.Sinks[i].ID, initialBackoffDelay)
	}

	if config.TsnetHostname == "" {
		for _, sink := range config.Sinks {
			if sink.UseTsnet {
				logger.ErrorContext("sink", sink.ID, "Sink uses tsnet but no tsnetHostname is configured")
			}
		}
	}
}

// getSinkStats retrieves statistics about a sink's database including the total event count
// and the most recent message content.
func getSinkStats(sink Sink) (int, string, error) {
	sinkDB, err, isErrDbSize := setupSql(sink.DatabasePath, false, "sink", sink.ID)
	if err != nil && !isErrDbSize {
		logger.ErrorContext("sink", sink.ID, "Failed to open sink database: %v", err)
		return 0, "", err
	}
	defer sinkDB.Close()

	var count int
	err = sinkDB.QueryRow("SELECT COUNT(*) FROM sink_events").Scan(&count)
	if err != nil {
		logger.ErrorContext("sink", sink.ID, "Failed to count sink events: %v", err)
		return 0, "", err
	}

	var lastMessage string
	err = sinkDB.QueryRow("SELECT content FROM sink_events ORDER BY id DESC LIMIT 1").Scan(&lastMessage)
	if err != nil && err != sql.ErrNoRows {
		logger.ErrorContext("sink", sink.ID, "Failed to get last message: %v", err)
		return count, "", err
	}

	return count, lastMessage, nil
}
