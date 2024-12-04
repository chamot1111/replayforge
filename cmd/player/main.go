package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
	"fmt"
	"runtime"
	"path/filepath"

	"github.com/chamot1111/replayforge/pkgs/lualibs"
	"github.com/chamot1111/replayforge/pkgs/playerplugin"
	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/chamot1111/replayforge/version"

	"github.com/Shopify/go-lua"
	"github.com/chamot1111/replayforge/internal/envparser"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vjeantet/grok"
	"tailscale.com/tsnet"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/cpu"
)

type Source struct {
	Name                      string
	RelayAuthenticationBearer string
	LuaVM                     *lua.State
	Sinks                     []playerplugin.Sink
	HookInterval              time.Duration
	LastHookTime              time.Time
	mu                        sync.Mutex
}

type SourceConfig struct {
	Name                      string   `json:"name"`
	RelayAuthenticationBearer string   `json:"relayAuthenticationBearer"`
	TransformScript           string   `json:"transformScript"`
	Sinks                     []string `json:"sinks"`
	HookInterval              string   `json:"hookInterval"`
}

type SinkConfig struct {
	Name   string          `json:"name"`
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Params json.RawMessage `json:"params"`
	Metadata          json.RawMessage `json:"metadata"`
}

var (
	configPath           string
	config               map[string]interface{}
	heartbeatIntervalMs  = 1000
	maxDbSize            = 100 * 1024 * 1024 // 100 MB
	relayUrl            string
	relayUrlStatusz     string
	sources             []Source
	sinks               map[string]playerplugin.Sink
	sinkConfigByName map[string]SinkConfig
	sinkChannels        sync.Map
	useTsnet            bool
	tsnetHostname       string
	globalExposedPort   int
	globalListenAddress string
	listenUsingTailscale bool
	staticFolderPath     string
	envName            string
	hostName           string
	portStatusZ        int
	useTsnetStatusZ   bool
)

func timerHandler(sourceID string) {
	var source *Source
	for i := range sources {
		if sources[i].Name == sourceID {
			source = &sources[i]
			break
		}
	}
	if source == nil {
		logger.Error("Source %s not found", sourceID)
		return
	}

	source.mu.Lock()
	defer source.mu.Unlock()

	// Check if it's time to run the hook based on interval
	if time.Since(source.LastHookTime) < source.HookInterval {
		return
	}
	source.LastHookTime = time.Now()

	// Check if timer_handler exists
	source.LuaVM.Global("TimerHandler")
	if !source.LuaVM.IsFunction(-1) {
		source.LuaVM.Pop(1)
		return
	}

	// Push emit function
	source.LuaVM.PushGoFunction(func(l *lua.State) int {
		sinkId, _ := l.ToString(1)
		emittedContent, _ := l.ToString(2)

		logger.Debug("Timer emitted content for sink %s: %s", sinkId, emittedContent)

		if ch, ok := sinkChannels.Load(sinkId); ok {
			ch.(chan string) <- emittedContent
		} else {
			logger.Error("Sink channel with ID %s not found", sinkId)
		}
		return 0
	})

	if err := source.LuaVM.ProtectedCall(1, 0, 0); err != nil {
		logger.Error("Error executing timer_handler for source %s: %v", source.Name, err)
	}
}

func init() {
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel != "" {
		logger.SetLogLevel(logLevel)
	}
	versionFlag := flag.Bool("v", false, "Affiche la version")
	flag.StringVar(&configPath, "c", "config.json", "Path to the configuration file")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("Version: %s\n", version.Version)
		os.Exit(0)
	}

	logger.Info("Config path: %s", configPath)
	configData, err := os.ReadFile(configPath)
	if err != nil {
		panic(err)
	}

	configDataStr, err := envparser.ProcessJSONWithEnvVars(string(configData))
	if err != nil {
		panic(err)
	}
	configData = []byte(configDataStr)

	err = json.Unmarshal(configData, &config)
	if err != nil {
		panic(err)
	}

	relayUrl = config["relayUrl"].(string)

	if statusUrl, ok := config["relayUrlStatusz"].(string); ok {
		relayUrlStatusz = statusUrl
		logger.Debug("Status URL set to %s", relayUrlStatusz)
	}

	if val, ok := config["envName"].(string); ok {
		envName = val
	}

	if val, ok := config["hostName"].(string); ok {
		hostName = val
	}
	if hostName == "" {
		hostname, err := os.Hostname()
		if err == nil {
			hostName = hostname
		}
	}

	// Check if tsnet is configured
	if tsnetConfig, ok := config["tsnet"].(map[string]interface{}); ok {
		useTsnet = true
		tsnetHostname = tsnetConfig["hostname"].(string)
	}

	// Get listenUsingTailscale from config
	if val, ok := config["listenUsingTailscale"].(bool); ok {
		listenUsingTailscale = val
	}

	// Get static folder path from config
	if val, ok := config["staticFolderPath"].(string); ok {
		staticFolderPath = val
	}

	if val, ok := config["portStatusZ"].(float64); ok {
		portStatusZ = int(val)
	}

	if val, ok := config["useTsnetStatusZ"].(bool); ok {
		useTsnetStatusZ = val
	}

	var sourcesConfig []SourceConfig
	if sourcesData, ok := config["sources"].([]interface{}); ok {
		for _, sourceData := range sourcesData {
			if sourceMap, ok := sourceData.(map[string]interface{}); ok {
				var sc SourceConfig
				sc.Name = sourceMap["name"].(string)
				sc.RelayAuthenticationBearer = sourceMap["relayAuthenticationBearer"].(string)
				if transformScript, ok := sourceMap["transformScript"].(string); ok {
					sc.TransformScript = transformScript
				} else if sinks, ok := sourceMap["sinks"].([]interface{}); ok {
					for _, sink := range sinks {
						sc.Sinks = append(sc.Sinks, sink.(string))
					}
				} else {
					logger.Fatal("Source %s must have either 'transformScript' or 'sinks' defined", sc.Name)
				}
				if hookInterval, ok := sourceMap["hookInterval"].(string); ok {
					sc.HookInterval = hookInterval
				}
				sourcesConfig = append(sourcesConfig, sc)
			}
		}
	} else {
		logger.Fatal("Failed to parse sources from config. Current type: %T", config["sources"])
	}

	var sinksConfig []SinkConfig
	sinksJSON, err := json.Marshal(config["sinks"])
	if err != nil {
		logger.Fatal("Failed to marshal sinks config: %v", err)
	}
	err = json.Unmarshal(sinksJSON, &sinksConfig)
	if err != nil {
		logger.Fatal("Failed to unmarshal sinks config: %v", err)
	}

	sinks = make(map[string]playerplugin.Sink)
	sinkChannels = sync.Map{}
	sinkConfigByName = make(map[string]SinkConfig)

	for _, sc := range sinksConfig {
		var sink playerplugin.Sink
		switch sc.Type {
		case "http":
			sink = &HttpSink{}
		case "db":
			sink = &SqliteSink{}
		case "alert":
			sink = &AlertSink{}
		case "log":
			sink = &LogSink{}
		case "notification":
			sink = &NotificationSink{}
		default:
			// Try to load a plugin for unknown sink types
			pluginPath := fmt.Sprintf("./%s_sink.so", sc.Type)
			loadedSink, err := LoadSinkPlugin(pluginPath)
			if err != nil {
				logger.Fatal("Failed to load sink plugin for type %s: %v", sc.Type, err)
			}
			sink = loadedSink
		}

		sinkConfig := playerplugin.SinkConfig{
			BaseSink: playerplugin.BaseSink{
				ID:   sc.ID,
				Type: sc.Type,
				Name: sc.Name,
				Metadata: map[string]interface{}{},
			},
			Params: sc.Params,
		}

		if len(sc.Metadata) > 0 {
			var metadata map[string]interface{}
			if err := json.Unmarshal(sc.Metadata, &metadata); err != nil {
				logger.Fatal("Failed to parse sink metadata: %v", err)
			}
			sinkConfig.BaseSink.Metadata = metadata
		}

		if err := sink.Init(sinkConfig, &sinkChannels); err != nil {
			logger.Fatal("Failed to initialize sink: %v", err)
		}

		if err := sink.Start(); err != nil {
			logger.Fatal("Failed to start sink: %v", err)
		}

		sinks[sc.Name] = sink
		sinkChannels.Store(sc.Name, make(chan string, 1000))
		sinkConfigByName[sc.Name] = sc
	}

	for sinkName, _ := range sinks {
		logger.Info("Sink: %s", sinkName)
	}

	for _, sc := range sourcesConfig {
		source := Source{
			Name:                      sc.Name,
			RelayAuthenticationBearer: sc.RelayAuthenticationBearer,
		}

		if sc.HookInterval != "" {
			duration, err := time.ParseDuration(sc.HookInterval)
			if err != nil {
				logger.Fatal("Invalid hook interval for source %s: %v", sc.Name, err)
			}
			source.HookInterval = duration
		}

		// Initialize Lua VM
		source.LuaVM = lua.NewState()
		lua.OpenLibraries(source.LuaVM)
		lualibs.RegisterLuaLibs(source.LuaVM)

		// Register grok parser
		g, _ := grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true})
		source.LuaVM.Register("grok_parse", func(l *lua.State) int {
			pattern, _ := l.ToString(1)
			text, _ := l.ToString(2)
			values, err := g.Parse(pattern, text)
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

		// Load the Lua script from file or use default script
		var scriptContent string
		if sc.TransformScript == "" {
			// Create default Lua script that emits content to all configured sinks
			defaultScript := "function Process(content, emit)\n"
			for _, sinkName := range sc.Sinks {
				defaultScript += fmt.Sprintf("    emit('%s', content)\n", sinkName)
			}
			defaultScript += "end"
			scriptContent = defaultScript
		} else {
			scriptBytes, err := os.ReadFile(sc.TransformScript)
			if err != nil {
				logger.Fatal("Failed to read transform script file %s: %v", sc.TransformScript, err)
			}
			scriptContent = string(scriptBytes)
		}

		if err := lua.DoString(source.LuaVM, scriptContent); err != nil {
			luaError := fmt.Sprintf("Failed to load Lua script for source %s with error: %v\nScript contents:\n%s", source.Name, err, scriptContent)
			logger.Fatal(luaError)
		}

		// Call init function if it exists
		source.LuaVM.Global("init")
		if source.LuaVM.IsFunction(-1) {
			if err := source.LuaVM.ProtectedCall(0, 0, 0); err != nil {
				logger.Fatal("Failed to call init function for source %s: %v", source.Name, err)
			}
		}
		source.LuaVM.Pop(1)

		// Attach sinks
		for _, sinkName := range sc.Sinks {
			if sink, ok := sinks[sinkName]; ok {
				source.Sinks = append(source.Sinks, sink)
			} else {
				logger.Fatal("Sink %s not found for source %s", sinkName, source.Name)
			}
		}

		sources = append(sources, source)
	}

	if relayUrl == "" {
		logger.Fatal("relayUrl must be specified in the config file")
	}

	if relayUrl != "" && !strings.HasSuffix(relayUrl, "/") {
		relayUrl += "/"
	}

	if exposedPort, ok := config["exposedPort"].(float64); ok {
		globalExposedPort = int(exposedPort)
	}
	if listenAddress, ok := config["listenAddress"].(string); ok {
		globalListenAddress = listenAddress
	}

	for _, source := range sources {
		maskedBearer := source.RelayAuthenticationBearer
		if len(maskedBearer) > 2 {
			maskedBearer = maskedBearer[0:2] + "***"
		}
		logger.Info("Source: %s, RelayAuthenticationBearer: %s", source.Name, maskedBearer)
	}
}

var (
	backoffDelays    = []int{1, 2, 5, 10}
	backoffIndex     = 0
	curBackoffToSkip = 0
)

func OnServerHeartbeat(source *Source, client *http.Client) {
	source.mu.Lock()
	defer source.mu.Unlock()

	if curBackoffToSkip > 0 {
		curBackoffToSkip--
		return
	}

	for {
		req, err := http.NewRequest("GET", relayUrl+"first-batch?limit=10", nil)
		if err != nil {
			logger.Error("Error creating request: %v", err)
			if backoffIndex < len(backoffDelays) {
				curBackoffToSkip = backoffDelays[backoffIndex]
				backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
			}
			return
		}

		req.Header.Set("RF-BUCKET", source.Name)
		req.Header.Set("Authorization", "Bearer "+source.RelayAuthenticationBearer)
		if envName != "" {
			req.Header.Set("RF-ENV-NAME", envName)
		}
		req.Header.Set("RF-HOSTNAME", hostName)

		resp, err := client.Do(req)
		if err != nil {
			logger.ErrorContext("source", source.Name, "Error fetching from relay: %v", err)
			if backoffIndex < len(backoffDelays) {
				curBackoffToSkip = backoffDelays[backoffIndex]
				backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
			}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == 404 {
			backoffIndex = 0
			return
		}
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			logger.ErrorContext("source", source.Name, "Error fetching data from relay server - HTTP Status: %d. Response body received: %s.", resp.StatusCode, string(body))
			if backoffIndex < len(backoffDelays) {
				curBackoffToSkip = backoffDelays[backoffIndex]
				backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
			}
			return
		}

		var responseBatch []map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&responseBatch)
		if err != nil {
			logger.Error("Error decoding response body: %v", err)
			if backoffIndex < len(backoffDelays) {
				curBackoffToSkip = backoffDelays[backoffIndex]
				backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
			}
			return
		}

		if len(responseBatch) == 0 {
			backoffIndex = 0
			return
		}

		var idsToAck []string

		for _, responseMap := range responseBatch {
			id, ok := responseMap["id"].(float64)
			if !ok {
				logger.Error("'id' not found in response or not a number")
				continue
			}
			idStr := fmt.Sprintf("%d", int(id))

			content, ok := responseMap["content"].(string)
			if !ok {
				logger.Error("'content' not found in response or not a string")
				continue
			}

			// Process the event through Lua VM
			source.LuaVM.Global("Process")
			if source.LuaVM.IsFunction(-1) {
				source.LuaVM.PushString(content)
				source.LuaVM.PushGoFunction(func(l *lua.State) int {
					sinkId, _ := l.ToString(1)
					emittedContent, _ := l.ToString(2)

					logger.Debug("Processed content for sink %s: %s", sinkId, emittedContent)

					if ch, ok := sinkChannels.Load(sinkId); ok {
					    ch.(chan string) <- emittedContent
					} else {
					    logger.Error("Sink channel with ID %s not found", sinkId)
					}

					return 0
				})

				if err := source.LuaVM.ProtectedCall(2, 1, 0); err != nil {
					logger.Error("Error calling process function for source %s: %v", source.Name, err)
					continue
				}

			} else {
				logger.Error("'process' function not found in Lua script for source %s", source.Name)
				source.LuaVM.Pop(1)
				continue
			}

			idsToAck = append(idsToAck, idStr)
		}

		if len(idsToAck) > 0 {
			// Acknowledge relay server
			ackBody, err := json.Marshal(map[string][]string{"ids": idsToAck})
			if err != nil {
				logger.Error("Error marshaling acknowledgment body: %v", err)
				if backoffIndex < len(backoffDelays) {
					curBackoffToSkip = backoffDelays[backoffIndex]
					backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
				}
				return
			}

			ackReq, err := http.NewRequest("DELETE", relayUrl+"acknowledge-batch", bytes.NewBuffer(ackBody))

			if err != nil {
				logger.Error("Error creating acknowledgment request: %v", err)
				if backoffIndex < len(backoffDelays) {
					curBackoffToSkip = backoffDelays[backoffIndex]
					backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
				}
				return
			}
			ackReq.Header.Set("RF-BUCKET", source.Name)
			ackReq.Header.Set("Authorization", "Bearer "+source.RelayAuthenticationBearer)
			ackReq.Header.Set("Content-Type", "application/json")
			if envName != "" {
				ackReq.Header.Set("RF-ENV-NAME", envName)
			}
			ackReq.Header.Set("RF-HOSTNAME", hostName)

			ackResp, err := client.Do(ackReq)
			if err != nil {
				logger.Error("Error sending acknowledgment: %v", err)
				if backoffIndex < len(backoffDelays) {
					curBackoffToSkip = backoffDelays[backoffIndex]
					backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
				}
				return
			}
			defer ackResp.Body.Close()

			if ackResp.StatusCode != 200 {
				logger.Error("Unexpected status code from acknowledgment: %d", ackResp.StatusCode)
				ackRespBody, err := io.ReadAll(ackResp.Body)
				if err != nil {
					logger.Error("Error reading acknowledgment response body: %v", err)
					if backoffIndex < len(backoffDelays) {
						curBackoffToSkip = backoffDelays[backoffIndex]
						backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
					}
					return
				}
				logger.Error("Acknowledgment response: %s", string(ackRespBody))
			}
		}

		// Reset backoff on successful processing
		backoffIndex = 0
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

   // Send node info to relay server for first source only
   if len(sources) > 0 {
    client := &http.Client{
     Timeout: time.Second,
    }

    req, err := http.NewRequest("POST", relayUrl+"node-info", bytes.NewBuffer(jsonData))
    if err != nil {
     logger.Error("Failed to create node info request: %v", err)
    } else {
     req.Header.Set("Content-Type", "application/json")
     req.Header.Set("RF-BUCKET", sources[0].Name)
     if sources[0].RelayAuthenticationBearer != "" {
         req.Header.Set("Authorization", "Bearer "+sources[0].RelayAuthenticationBearer)
     }
     if envName != "" {
      req.Header.Set("RF-ENV-NAME", envName)
     }
     req.Header.Set("RF-HOSTNAME", hostName)

     resp, err := client.Do(req)
     if err != nil {
      logger.Error("Failed to send node info: %v", err)
     } else {
      resp.Body.Close()
     }
    }
   }

   time.Sleep(time.Minute)
  }
 }()
}

func main() {
	var s *tsnet.Server
	var client *http.Client

	if useTsnet {
		s = &tsnet.Server{
			Hostname: tsnetHostname,
		}
		client = s.HTTPClient()
	} else {
		client = &http.Client{}
	}

	// Create a reverse proxy if globalExposedPort and globalListenAddress are defined
	if globalExposedPort != 0 {
		mux := http.NewServeMux()

		// Serve static files if path is configured
		if staticFolderPath != "" {
			fs := http.FileServer(http.Dir(staticFolderPath))
			mux.Handle("/static/", http.StripPrefix("/static/", fs))
		}

		// Add ping route for testing
		mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("pong"))
		})

		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// Try to serve static file first
			if staticFolderPath != "" {
				filePath := filepath.Join(staticFolderPath, filepath.Clean(r.URL.Path))
				if !strings.HasPrefix(filePath, staticFolderPath) {
					http.Error(w, "Invalid path", http.StatusForbidden)
					return
				}
				if _, err := os.Stat(filePath); err == nil {
					fs := http.FileServer(http.Dir(staticFolderPath))
					fs.ServeHTTP(w, r)
					return
				}
			}

			// Fall back to sink handling
			path := strings.TrimPrefix(r.URL.Path, "/")
			parts := strings.SplitN(path, "/", 2)
			sinkID := parts[0]

			if sink, ok := sinks[sinkID]; ok {
				if port, exposed := sink.GetExposedPort(); exposed {
					proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: fmt.Sprintf("localhost:%d", port)})
					proxy.ServeHTTP(w, r)
				} else {
					http.Error(w, "Sink does not expose a port", http.StatusNotFound)
				}
			} else {
				http.NotFound(w, r)
			}
		})

		mux.HandleFunc("/sink/list", func(w http.ResponseWriter, r *http.Request) {
			sinkList := make([]map[string]interface{}, 0)
			for id, sinkConfig := range sinkConfigByName {
				sinkInfo := map[string]interface{}{
					"id":       id,
					"type":     sinkConfig.Type,
					"metadata": map[string]interface{}{},
				}
				if len(sinkConfig.Metadata) > 0 {
					var metadata map[string]interface{}
					if err := json.Unmarshal(sinkConfig.Metadata, &metadata); err == nil {
						sinkInfo["metadata"] = metadata
					}
				}
				sinkList = append(sinkList, sinkInfo)
			}

			json.NewEncoder(w).Encode(sinkList)
		})

		mux.HandleFunc("/sink/find/", func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimPrefix(r.URL.Path, "/sink/find/")
			if id == "" {
				http.Error(w, "id parameter is required", http.StatusBadRequest)
				return
			}

			for _, sinkConfig := range sinkConfigByName {
				if sinkConfig.ID == id {
					sinkInfo := map[string]interface{}{
						"id":       sinkConfig.ID,
						"type":     sinkConfig.Type,
						"metadata": map[string]interface{}{},
					}
					if len(sinkConfig.Metadata) > 0 {
						var metadata map[string]interface{}
						if err := json.Unmarshal(sinkConfig.Metadata, &metadata); err == nil {
							sinkInfo["metadata"] = metadata
						}
					}
					json.NewEncoder(w).Encode(sinkInfo)
					return
				}
			}

			http.Error(w, "Sink not found", http.StatusNotFound)
		})

		mux.HandleFunc("/relay-proxy/", func(w http.ResponseWriter, r *http.Request) {
			logger.Trace("Handling relay-proxy request: %s %s", r.Method, r.URL.Path)

			relayUrlParsed, err := url.Parse(relayUrlStatusz)
			if err != nil {
				logger.Error("Failed to parse relay URL: %v", err)
				http.Error(w, "Error parsing relay URL", http.StatusInternalServerError)
				return
			}

			if relayUrlParsed.Scheme == "" {
				relayUrlParsed.Scheme = "http"
			}

			proxy := httputil.NewSingleHostReverseProxy(relayUrlParsed)
			originalPath := r.URL.Path
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/relay-proxy")
			logger.Trace("Rewrote path from %s to %s", originalPath, r.URL.Path)

			// Copy headers from original request
			for k, v := range r.Header {
				for _, val := range v {
					proxy.Director = func(req *http.Request) {
						logger.Trace("Setting proxy request details - URL: %s, Host: %s, Path: %s",
							relayUrlParsed.String(), relayUrlParsed.Host, r.URL.Path)
						req.URL.Scheme = relayUrlParsed.Scheme
						req.URL.Host = relayUrlParsed.Host
						req.Host = relayUrlParsed.Host
						req.URL.Path = r.URL.Path
						req.Header.Set(k, val)
						logger.Trace("Set header %s = %s", k, val)
					}
				}
			}

			if useTsnet {
				logger.Trace("Using tsnet transport for proxy")
				proxy.Transport = client.Transport
			}

			logger.Trace("Forwarding request to relay server")
			proxy.ServeHTTP(w, r)
			logger.Trace("Completed forwarding request")
		})

		go func() {
			if listenUsingTailscale && useTsnet {
				logger.Info("Starting reverse proxy using Tailscale on %s:%d", globalListenAddress, globalExposedPort)
			} else {
				logger.Info("Starting reverse proxy on %s:%d", globalListenAddress, globalExposedPort)
			}
			srv := &http.Server{
				Addr:    fmt.Sprintf("%s:%d", globalListenAddress, globalExposedPort),
				Handler: mux,
			}
			if listenUsingTailscale && useTsnet {
				ln, err := s.Listen("tcp", srv.Addr)
				if err != nil {
					logger.Error("Error creating tailscale listener: %v", err)
					return
				}
				if err := srv.Serve(ln); err != nil {
					logger.Error("Error serving through tailscale: %v", err)
				}
			} else {
				if err := srv.ListenAndServe(); err != nil {
					logger.Error("Error starting reverse proxy: %v", err)
				}
			}
		}()
	}

	for sinkName, sink := range sinks {
		sinkName := sinkName // Shadow variable to avoid closure issues
		sink := sink

		go func(sinkName string, sink playerplugin.Sink) {
			ch, _ := sinkChannels.Load(sinkName)
			chTyped := ch.(chan string)
			for msg := range chTyped {
				var decodedData map[string]interface{}
				err := json.Unmarshal([]byte(msg), &decodedData)
				if err != nil {
					logger.Error("Error decoding processed JSON data: %v", err)
					continue
				}

				method, _ := decodedData["method"].(string)
				path, _ := decodedData["path"].(string)
				requestBody, _ := decodedData["body"].(string)
				headers, _ := decodedData["headers"].(map[string]interface{})
				params, _ := decodedData["params"].(map[string]interface{})

				err = sink.Execute(method, path, []byte(requestBody), headers, params, &sinkChannels)
				if err != nil {
					logger.Error("Error executing sink operation: %v", err)
				}
			}
		}(sinkName, sink)
	}

	for i := range sources {
		source := &sources[i] // Use pointer to avoid copying mutex
		if source.HookInterval > 0 {
			go func(s *Source) {
				ticker := time.NewTicker(s.HookInterval)
				defer ticker.Stop()

				for range ticker.C {
					timerHandler(s.Name)
				}
			}(source)
		}
	}

	startNodeInfoReporting()

	ticker := time.NewTicker(time.Duration(heartbeatIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		for i := range sources {
			OnServerHeartbeat(&sources[i], client)
		}
	}
}
