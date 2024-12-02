package playerplugin

import (
	"encoding/json"
	"sync"
)

type BaseSink struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	Name string `json:"bucketName"`
	Metadata map[string]interface{} `json:"metadata"`
}

type SinkConfig struct {
	BaseSink
	Params json.RawMessage `json:"params"`
	LuaScript string       `json:"luaScript"`
}

type Sink interface {
	Init(config SinkConfig, sinkChannels *sync.Map) error
	Start() error
	Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels *sync.Map) error
	Close() error
	GetID() string
	GetExposedPort() (int, bool)
}

type SinkPlugin interface {
	NewSink() (Sink, error)
}
