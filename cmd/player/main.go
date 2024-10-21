package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"github.com/chamot1111/replayforge/playerplugin"

	_ "github.com/mattn/go-sqlite3"
	"tailscale.com/tsnet"
	"github.com/Shopify/go-lua"
	"github.com/vjeantet/grok"
)

type Bucket struct {
	Name                      string
	RelayAuthenticationBearer string
	Sink                      playerplugin.Sink
	LuaVM                     *lua.State
}

type BucketConfig struct {
	Name                      string          `json:"name"`
	RelayAuthenticationBearer string          `json:"relayAuthenticationBearer"`
	SinkType                  string          `json:"sinkType"`
	ID                        string          `json:"id"`
	Params                    json.RawMessage `json:"params"`
	LuaScript                 string          `json:"luaScript"`
}

var (
	configPath          string
	config              map[string]interface{}
	heartbeatIntervalMs = 100
	maxDbSize           = 100 * 1024 * 1024 // 100 MB
	relayUrl            string
	buckets             []Bucket
	useTsnet            bool
	tsnetHostname       string
)

func init() {
	configPath = "config.json"
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

	var bucketsConfig []BucketConfig
	bucketsJSON, err := json.Marshal(config["buckets"])
	if err != nil {
		panic(fmt.Sprintf("Failed to marshal buckets config: %v", err))
	}
	err = json.Unmarshal(bucketsJSON, &bucketsConfig)
	if err != nil {
		panic(fmt.Sprintf("Failed to unmarshal buckets config: %v", err))
	}

	for _, bc := range bucketsConfig {
		bucket := Bucket{
			Name:                      bc.Name,
			RelayAuthenticationBearer: bc.RelayAuthenticationBearer,
		}

		var sink playerplugin.Sink
		switch bc.SinkType {
		case "http":
			sink = &HttpSink{}
		case "db":
			sink = &SqliteSink{}
		default:
			// Try to load a plugin for unknown sink types
			pluginPath := fmt.Sprintf("./%s_sink.so", bc.SinkType)
			loadedSink, err := LoadSinkPlugin(pluginPath)
			if err != nil {
				panic(fmt.Sprintf("Failed to load sink plugin for type %s: %v", bc.SinkType, err))
			}
			sink = loadedSink
		}

		sinkConfig := playerplugin.SinkConfig{
			BaseSink: playerplugin.BaseSink{
				ID:         bc.ID,
				Type:       bc.SinkType,
				BucketName: bucket.Name,
			},
			Params:    bc.Params,
			LuaScript: bc.LuaScript,
		}

		if err := sink.Init(sinkConfig); err != nil {
			panic(fmt.Sprintf("Failed to initialize sink: %v", err))
		}

		if err := sink.Start(); err != nil {
			panic(fmt.Sprintf("Failed to start sink: %v", err))
		}

		bucket.Sink = sink

		// Initialize Lua VM
		bucket.LuaVM = lua.NewState()
		lua.OpenLibraries(bucket.LuaVM)

		// Register grok parser
		g, _ := grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true})
		bucket.LuaVM.Register("grok_parse", func(l *lua.State) int {
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
		if bc.LuaScript == "" {
			bc.LuaScript = `
				function process(content, emit)
					emit(content)
				end
			`
		}
		if err := lua.DoString(bucket.LuaVM, bc.LuaScript); err != nil {
			panic(fmt.Sprintf("Failed to load Lua script for bucket %s: %v", bucket.Name, err))
		}

		// Call init function if it exists
		bucket.LuaVM.Global("init")
		if bucket.LuaVM.IsFunction(-1) {
			if err := bucket.LuaVM.ProtectedCall(0, 0, 0); err != nil {
				panic(fmt.Sprintf("Failed to call init function for bucket %s: %v", bucket.Name, err))
			}
		}
		bucket.LuaVM.Pop(1)

		buckets = append(buckets, bucket)
	}

	if relayUrl == "" {
		panic("relayUrl must be specified in the config file")
	}

	if relayUrl != "" && !strings.HasSuffix(relayUrl, "/") {
		relayUrl += "/"
	}
}

func OnServerHeartbeat(bucket Bucket, client *http.Client) {
	req, err := http.NewRequest("GET", relayUrl+"first", nil)
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return
	}

	req.Header.Set("RF-BUCKET", bucket.Name)
	req.Header.Set("Authorization", "Bearer "+bucket.RelayAuthenticationBearer)

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
	bucket.LuaVM.Global("process")
	if bucket.LuaVM.IsFunction(-1) {
		bucket.LuaVM.PushString(content)
		bucket.LuaVM.PushGoFunction(func(l *lua.State) int {
			emittedContent, _ := l.ToString(-1)

			fmt.Printf("Processed content: %s\n", emittedContent)

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

			err = bucket.Sink.Execute(method, path, []byte(requestBody), headers, params)
			if err != nil {
				fmt.Printf("Error executing sink operation for bucket %s: %v\n", bucket.Name, err)
				// Don't return, continue to the next iteration
			}

			return 0
		})

		if err := bucket.LuaVM.ProtectedCall(2, 1, 0); err != nil {
			fmt.Printf("Error calling process function for bucket %s: %v\n", bucket.Name, err)
		}

	} else {
		fmt.Printf("Error: 'process' function not found in Lua script for bucket %s\n", bucket.Name)
		bucket.LuaVM.Pop(1)
	}

	// Acknowledge relay server
	acknowledgeUrl := fmt.Sprintf("%sacknowledge?id=%d", relayUrl, idInt)
	ackReq, err := http.NewRequest("DELETE", acknowledgeUrl, nil)
	if err != nil {
		fmt.Printf("Error creating acknowledgment request: %v\n", err)
		return
	}
	ackReq.Header.Set("RF-BUCKET", bucket.Name)
	ackReq.Header.Set("Authorization", "Bearer "+bucket.RelayAuthenticationBearer)

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

	ticker := time.NewTicker(time.Duration(heartbeatIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		for _, bucket := range buckets {
			OnServerHeartbeat(bucket, client)
		}
	}
}
