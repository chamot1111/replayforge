package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/chamot1111/replayforge/lualibs"
	"github.com/chamot1111/replayforge/playerplugin"

	"github.com/Shopify/go-lua"
	"github.com/chamot1111/replayforge/internal/envparser"
	_ "github.com/mattn/go-sqlite3"
	"github.com/vjeantet/grok"
	"tailscale.com/tsnet"
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
}

var (
	configPath          string
	config              map[string]interface{}
	heartbeatIntervalMs = 1000
	maxDbSize           = 100 * 1024 * 1024 // 100 MB
	relayUrl            string
	sources             []Source
	sinks               map[string]playerplugin.Sink
	sinkChannels        map[string]chan string
	useTsnet            bool
	tsnetHostname       string
	globalExposedPort   int
	globalListenAddress string
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
		fmt.Printf("Source %s not found\n", sourceID)
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
	source.LuaVM.Global("timer_handler")
	if !source.LuaVM.IsFunction(-1) {
		source.LuaVM.Pop(1)
		return
	}

	// Push emit function
	source.LuaVM.PushGoFunction(func(l *lua.State) int {
		sinkId, _ := l.ToString(1)
		emittedContent, _ := l.ToString(2)

		fmt.Printf("Timer emitted content for sink %s: %s\n", sinkId, emittedContent)

		if ch, ok := sinkChannels[sinkId]; ok {
			ch <- emittedContent
		} else {
			fmt.Printf("Error: Sink channel with ID %s not found\n", sinkId)
		}
		return 0
	})

	if err := source.LuaVM.ProtectedCall(1, 0, 0); err != nil {
		fmt.Printf("Error executing timer_handler for source %s: %v\n", source.Name, err)
	}
}

func init() {
	flag.StringVar(&configPath, "c", "config.json", "Path to the configuration file")
	flag.Parse()
	fmt.Printf("Config path: %s\n", configPath)
	configData, err := os.ReadFile(configPath)
	if err != nil {
		panic(err)
	}

	configDataStr, err := envparser.ProcessJSONWithEnvVars(string(configData))
	if err != nil {
		panic(fmt.Errorf("error processing environment variables in config: %w", err))
	}
	configData = []byte(configDataStr)

	err = json.Unmarshal(configData, &config)
	if err != nil {
		panic(err)
	}

	relayUrl = config["relayUrl"].(string)

	// Check if tsnet is configured
	if tsnetConfig, ok := config["tsnet"].(map[string]interface{}); ok {
		useTsnet = true
		tsnetHostname = tsnetConfig["hostname"].(string)
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
					panic(fmt.Sprintf("Source %s must have either 'transformScript' or 'sinks' defined", sc.Name))
				}
				if hookInterval, ok := sourceMap["hookInterval"].(string); ok {
					sc.HookInterval = hookInterval
				}
				sourcesConfig = append(sourcesConfig, sc)
			}
		}
	} else {
		panic(fmt.Sprintf("Failed to parse sources from config. Current type: %T", config["sources"]))
	}

	var sinksConfig []SinkConfig
	sinksJSON, err := json.Marshal(config["sinks"])
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal sinks config: %v", err))
	}
	err = json.Unmarshal(sinksJSON, &sinksConfig)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal sinks config: %v", err))
	}

	sinks = make(map[string]playerplugin.Sink)
	sinkChannels = make(map[string]chan string)

	for _, sc := range sinksConfig {
		var sink playerplugin.Sink
		switch sc.Type {
		case "http":
			sink = &HttpSink{}
		case "db":
			sink = &SqliteSink{}
		case "log":
			sink = &LogSink{}
		case "notification":
			sink = &NotificationSink{}
		default:
			// Try to load a plugin for unknown sink types
			pluginPath := fmt.Sprintf("./%s_sink.so", sc.Type)
			loadedSink, err := LoadSinkPlugin(pluginPath)
			if err != nil {
				panic(fmt.Sprintf("Failed to load sink plugin for type %s: %v", sc.Type, err))
			}
			sink = loadedSink
		}

		sinkConfig := playerplugin.SinkConfig{
			BaseSink: playerplugin.BaseSink{
				ID:   sc.ID,
				Type: sc.Type,
				Name: sc.Name,
			},
			Params: sc.Params,
		}

		if err := sink.Init(sinkConfig); err != nil {
			panic(fmt.Sprintf("Failed to initialize sink: %v", err))
		}

		if err := sink.Start(); err != nil {
			panic(fmt.Sprintf("Failed to start sink: %v", err))
		}

		sinks[sc.Name] = sink
		sinkChannels[sc.Name] = make(chan string, 1000)
	}

	for sinkName, _ := range sinks {
		fmt.Printf("Sink: %s\n", sinkName)
	}

	for _, sc := range sourcesConfig {
		source := Source{
			Name:                      sc.Name,
			RelayAuthenticationBearer: sc.RelayAuthenticationBearer,
		}

		if sc.HookInterval != "" {
			duration, err := time.ParseDuration(sc.HookInterval)
			if err != nil {
				panic(fmt.Sprintf("Invalid hook interval for source %s: %v", sc.Name, err))
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
				panic(fmt.Sprintf("Failed to read transform script file %s: %v", sc.TransformScript, err))
			}
			scriptContent = string(scriptBytes)
		}

		if err := lua.DoString(source.LuaVM, scriptContent); err != nil {
			luaError := fmt.Sprintf("Failed to load Lua script for source %s with error: %v\nScript contents:\n%s", source.Name, err, scriptContent)
			panic(luaError)
		}

		// Call init function if it exists
		source.LuaVM.Global("init")
		if source.LuaVM.IsFunction(-1) {
			if err := source.LuaVM.ProtectedCall(0, 0, 0); err != nil {
				panic(fmt.Sprintf("Failed to call init function for source %s: %v", source.Name, err))
			}
		}
		source.LuaVM.Pop(1)

		// Attach sinks
		for _, sinkName := range sc.Sinks {
			if sink, ok := sinks[sinkName]; ok {
				source.Sinks = append(source.Sinks, sink)
			} else {
				panic(fmt.Sprintf("Sink %s not found for source %s", sinkName, source.Name))
			}
		}

		sources = append(sources, source)
	}

	if relayUrl == "" {
		panic("relayUrl must be specified in the config file")
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
		fmt.Printf("Source: %s, RelayAuthenticationBearer: %s\n", source.Name, source.RelayAuthenticationBearer)
	}
}

var (
	backoffDelays    = []int{1, 2, 5, 10}
	backoffIndex     = 0
	curBackoffToSkip = 0
)

func OnServerHeartbeat(source Source, client *http.Client) {
	source.mu.Lock()
	defer source.mu.Unlock()

	if curBackoffToSkip > 0 {
		curBackoffToSkip--
		return
	}

	for {
		req, err := http.NewRequest("GET", relayUrl+"first-batch?limit=10", nil)
		if err != nil {
			fmt.Printf("Error creating request: %v\n", err)
			if backoffIndex < len(backoffDelays) {
				curBackoffToSkip = backoffDelays[backoffIndex]
				backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
			}
			return
		}

		req.Header.Set("RF-BUCKET", source.Name)
		req.Header.Set("Authorization", "Bearer "+source.RelayAuthenticationBearer)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("Error fetching from relay: %v\n", err)
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
			fmt.Printf("Unexpected status code: %d\n", resp.StatusCode)
			if backoffIndex < len(backoffDelays) {
				curBackoffToSkip = backoffDelays[backoffIndex]
				backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
			}
			return
		}

		var responseBatch []map[string]interface{}
		err = json.NewDecoder(resp.Body).Decode(&responseBatch)
		if err != nil {
			fmt.Printf("Error decoding response body: %v\n", err)
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
				fmt.Println("Error: 'id' not found in response or not a number")
				continue
			}
			idStr := fmt.Sprintf("%d", int(id))

			content, ok := responseMap["content"].(string)
			if !ok {
				fmt.Println("Error: 'content' not found in response or not a string")
				continue
			}

			// Process the event through Lua VM
			source.LuaVM.Global("Process")
			if source.LuaVM.IsFunction(-1) {
				source.LuaVM.PushString(content)
				source.LuaVM.PushGoFunction(func(l *lua.State) int {
					sinkId, _ := l.ToString(1)
					emittedContent, _ := l.ToString(2)

					fmt.Printf("Processed content for sink %s: %s\n", sinkId, emittedContent)

					if ch, ok := sinkChannels[sinkId]; ok {
						ch <- emittedContent
					} else {
						fmt.Printf("Error: Sink channel with ID %s not found\n", sinkId)
					}

					return 0
				})

				if err := source.LuaVM.ProtectedCall(2, 1, 0); err != nil {
					fmt.Printf("Error calling process function for source %s: %v\n", source.Name, err)
					continue
				}

			} else {
				fmt.Printf("Error: 'process' function not found in Lua script for source %s\n", source.Name)
				source.LuaVM.Pop(1)
				continue
			}

			idsToAck = append(idsToAck, idStr)
		}

		if len(idsToAck) > 0 {
			// Acknowledge relay server
			ackBody, err := json.Marshal(map[string][]string{"ids": idsToAck})
			if err != nil {
				fmt.Printf("Error marshaling acknowledgment body: %v\n", err)
				if backoffIndex < len(backoffDelays) {
					curBackoffToSkip = backoffDelays[backoffIndex]
					backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
				}
				return
			}

			ackReq, err := http.NewRequest("DELETE", relayUrl+"acknowledge-batch", bytes.NewBuffer(ackBody))

			if err != nil {
				fmt.Printf("Error creating acknowledgment request: %v\n", err)
				if backoffIndex < len(backoffDelays) {
					curBackoffToSkip = backoffDelays[backoffIndex]
					backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
				}
				return
			}
			ackReq.Header.Set("RF-BUCKET", source.Name)
			ackReq.Header.Set("Authorization", "Bearer "+source.RelayAuthenticationBearer)
			ackReq.Header.Set("Content-Type", "application/json")

			ackResp, err := client.Do(ackReq)
			if err != nil {
				fmt.Printf("Error sending acknowledgment: %v\n", err)
				if backoffIndex < len(backoffDelays) {
					curBackoffToSkip = backoffDelays[backoffIndex]
					backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
				}
				return
			}
			defer ackResp.Body.Close()

			if ackResp.StatusCode != 200 {
				fmt.Printf("Unexpected status code from acknowledgment: %d\n", ackResp.StatusCode)
				ackRespBody, err := io.ReadAll(ackResp.Body)
				if err != nil {
					fmt.Printf("Error reading acknowledgment response body: %v\n", err)
					if backoffIndex < len(backoffDelays) {
						curBackoffToSkip = backoffDelays[backoffIndex]
						backoffIndex = min(backoffIndex+1, len(backoffDelays)-1)
					}
					return
				}
				fmt.Printf("Acknowledgment response: %s\n", string(ackRespBody))
			}
		}

		// Reset backoff on successful processing
		backoffIndex = 0
	}
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
	if globalExposedPort != 0 && globalListenAddress != "" {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			sinkID := strings.TrimPrefix(r.URL.Path, "/")
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

		go func() {
			fmt.Printf("Starting reverse proxy on %s:%d\n", globalListenAddress, globalExposedPort)
			if err := http.ListenAndServe(fmt.Sprintf("%s:%d", globalListenAddress, globalExposedPort), nil); err != nil {
				fmt.Printf("Error starting reverse proxy: %v\n", err)
			}
		}()
	}

	for sinkName, sink := range sinks {
		sinkName := sinkName // Shadow variable to avoid closure issues
		sink := sink

		go func(sinkName string, sink playerplugin.Sink) {
			ch := sinkChannels[sinkName]
			for msg := range ch {
				var decodedData map[string]interface{}
				err := json.Unmarshal([]byte(msg), &decodedData)
				if err != nil {
					fmt.Printf("Error decoding processed JSON data: %v\n", err)
					continue
				}

				method, _ := decodedData["method"].(string)
				path, _ := decodedData["path"].(string)
				requestBody, _ := decodedData["body"].(string)
				headers, _ := decodedData["headers"].(map[string]interface{})
				params, _ := decodedData["params"].(map[string]interface{})

				err = sink.Execute(method, path, []byte(requestBody), headers, params, sinkChannels)
				if err != nil {
					fmt.Printf("Error executing sink operation: %v\n", err)
				}
			}
		}(sinkName, sink)
	}

	for _, source := range sources {
		source := source // Shadow variable to avoid closure issues
		if source.HookInterval > 0 {
			go func() {
				ticker := time.NewTicker(source.HookInterval)
				defer ticker.Stop()

				for range ticker.C {
					timerHandler(source.Name)
				}
			}()
		}
	}

	ticker := time.NewTicker(time.Duration(heartbeatIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		for _, source := range sources {
			OnServerHeartbeat(source, client)
		}
	}
}
