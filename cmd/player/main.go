package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"flag"
	"net/http/httputil"
	"net/url"
	"github.com/chamot1111/replayforge/playerplugin"

	_ "github.com/mattn/go-sqlite3"
	"tailscale.com/tsnet"
	"github.com/Shopify/go-lua"
	"github.com/vjeantet/grok"
)

type Source struct {
	Name                      string
	RelayAuthenticationBearer string
	LuaVM                     *lua.State
	Sinks                     []playerplugin.Sink
}

type SourceConfig struct {
	Name                      string   `json:"name"`
	RelayAuthenticationBearer string   `json:"relayAuthenticationBearer"`
	LuaScript                 string   `json:"luaScript"`
	Sinks                     []string `json:"sinks"`
}

type SinkConfig struct {
	Name     string          `json:"name"`
	Type     string          `json:"type"`
	ID       string          `json:"id"`
	Params   json.RawMessage `json:"params"`
}

var (
	configPath          string
	config              map[string]interface{}
	heartbeatIntervalMs = 100
	maxDbSize           = 100 * 1024 * 1024 // 100 MB
	relayUrl            string
	sources             []Source
	sinks               map[string]playerplugin.Sink
	useTsnet            bool
	tsnetHostname       string
	globalExposedPort   int
	globalListenAddress string
)

func init() {
	flag.StringVar(&configPath, "c", "config.json", "Path to the configuration file")
	flag.Parse()
	fmt.Printf("Config path: %s\n", configPath)
	configData, err := os.ReadFile(configPath)
	if err != nil {
		panic(err)
	}
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
				if luaScript, ok := sourceMap["luaScript"].(string); ok {
					sc.LuaScript = luaScript
				} else if sinks, ok := sourceMap["sinks"].([]interface{}); ok {
					for _, sink := range sinks {
						sc.Sinks = append(sc.Sinks, sink.(string))
					}
				} else {
					panic(fmt.Sprintf("Source %s must have either 'luaScript' or 'sinks' defined", sc.Name))
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
	for _, sc := range sinksConfig {
		var sink playerplugin.Sink
		switch sc.Type {
		case "http":
			sink = &HttpSink{}
		case "db":
			sink = &SqliteSink{}
		case "log":
			sink = &LogSink{}
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
	}

	for sinkName, _ := range sinks {
		fmt.Printf("Sink: %s\n", sinkName)
	}

	for _, sc := range sourcesConfig {
		source := Source{
			Name:                      sc.Name,
			RelayAuthenticationBearer: sc.RelayAuthenticationBearer,
		}

		// Initialize Lua VM
		source.LuaVM = lua.NewState()
		lua.OpenLibraries(source.LuaVM)

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
		// Load the Lua script or use default script
		if sc.LuaScript == "" {
			// Create default Lua script that emits content to all configured sinks
			defaultScript := "function process(content, emit)\n"
			for _, sinkName := range sc.Sinks {
				defaultScript += fmt.Sprintf("    emit('%s', content)\n", sinkName)
			}
			defaultScript += "end"
			sc.LuaScript = defaultScript
		}
		if err := lua.DoString(source.LuaVM, sc.LuaScript); err != nil {
			panic(fmt.Sprintf("Failed to load Lua script for source %s: %v", source.Name, err))
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

func OnServerHeartbeat(source Source, client *http.Client) {
	req, err := http.NewRequest("GET", relayUrl+"first", nil)
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return
	}

	req.Header.Set("RF-BUCKET", source.Name)
	req.Header.Set("Authorization", "Bearer "+source.RelayAuthenticationBearer)

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error fetching from relay: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return
	}
	if resp.StatusCode != 200 {
		fmt.Printf("Unexpected status code: %d\n", resp.StatusCode)
		return
	}

	var responseMap map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&responseMap)
	if err != nil {
		fmt.Printf("Error decoding response body: %v\n", err)
		return
	}

	id, ok := responseMap["id"].(float64)
	if !ok {
		fmt.Println("Error: 'id' not found in response or not a number")
		return
	}
	idInt := int(id)

	content, ok := responseMap["content"].(string)
	if !ok {
		fmt.Println("Error: 'content' not found in response or not a string")
		return
	}
	// Process the event through Lua VM
	source.LuaVM.Global("process")
	if source.LuaVM.IsFunction(-1) {
		source.LuaVM.PushString(content)
		source.LuaVM.PushGoFunction(func(l *lua.State) int {
			sinkId, _ := l.ToString(1)
			emittedContent, _ := l.ToString(2)

			fmt.Printf("Processed content for sink %s: %s\n", sinkId, emittedContent)

			// Parse the processed content
			var decodedData map[string]interface{}
			err = json.Unmarshal([]byte(emittedContent), &decodedData)
			if err != nil {
				fmt.Printf("Error decoding processed JSON data: %v\n", err)
				return 0
			}

			method, _ := decodedData["method"].(string)
			path, _ := decodedData["path"].(string)
			requestBody, _ := decodedData["body"].(string)
			headers, _ := decodedData["headers"].(map[string]interface{})
			params, _ := decodedData["params"].(map[string]interface{})

			sinkFound := false
			for _, sink := range source.Sinks {
				if sink.GetID() == sinkId {
					err = sink.Execute(method, path, []byte(requestBody), headers, params)
					if err != nil {
						fmt.Printf("Error executing sink operation for source %s and sink %s: %v\n", source.Name, sinkId, err)
					}
					sinkFound = true
					break
				}
			}
			if !sinkFound {
				fmt.Printf("Error: Sink with ID %s not found for source %s\n", sinkId, source.Name)
			}

			return 0
		})

		if err := source.LuaVM.ProtectedCall(2, 1, 0); err != nil {
			fmt.Printf("Error calling process function for source %s: %v\n", source.Name, err)
		}

	} else {
		fmt.Printf("Error: 'process' function not found in Lua script for source %s\n", source.Name)
		source.LuaVM.Pop(1)
	}

	// Acknowledge relay server
	acknowledgeUrl := fmt.Sprintf("%sacknowledge?id=%d", relayUrl, idInt)
	ackReq, err := http.NewRequest("DELETE", acknowledgeUrl, nil)
	if err != nil {
		fmt.Printf("Error creating acknowledgment request: %v\n", err)
		return
	}
	ackReq.Header.Set("RF-BUCKET", source.Name)
	ackReq.Header.Set("Authorization", "Bearer "+source.RelayAuthenticationBearer)

	ackResp, err := client.Do(ackReq)
	if err != nil {
		fmt.Printf("Error sending acknowledgment: %v\n", err)
		return
	}
	defer ackResp.Body.Close()

	if ackResp.StatusCode != 200 {
		fmt.Printf("Unexpected status code from acknowledgment: %d\n", ackResp.StatusCode)
		ackBody, err := io.ReadAll(ackResp.Body)
		if err != nil {
			fmt.Printf("Error reading acknowledgment response body: %v\n", err)
			return
		}
		fmt.Printf("Acknowledgment response: %s\n", string(ackBody))
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
					// target := fmt.Sprintf("http://localhost:%d", port)
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

	ticker := time.NewTicker(time.Duration(heartbeatIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		for _, source := range sources {
			OnServerHeartbeat(source, client)
		}
	}
}
