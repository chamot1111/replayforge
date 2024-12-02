package main

import (
	"encoding/json"
	"fmt"
	"github.com/chamot1111/replayforge/pkgs/playerplugin"
	"github.com/chamot1111/replayforge/pkgs/logger"
	"sync"
)

type LogSink struct {
	BucketName  string
	LogLevel    string
	LogFilePath string
	ID          string
}

func (s *LogSink) Init(config playerplugin.SinkConfig, sinkChannels *sync.Map) error {
	s.BucketName = config.Name

	var params map[string]interface{}
	if err := json.Unmarshal(config.Params, &params); err != nil {
		logger.Error("Failed to unmarshal params: %v", err)
		return fmt.Errorf("failed to unmarshal params: %v", err)
	}

	if logLevel, ok := params["logLevel"].(string); ok {
		s.LogLevel = logLevel
	} else {
		s.LogLevel = "info"
	}

	if logFilePath, ok := params["logFilePath"].(string); ok {
		s.LogFilePath = logFilePath
	} else {
		s.LogFilePath = "./logs/example.log"
	}

	s.ID = config.ID
	return nil
}

func (s *LogSink) Start() error {
	return nil
}

func (s *LogSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels *sync.Map) error {
	logger.Info("Method: %s", method)
	logger.Info("Path: %s", path)
	logger.Info("Body: %s", string(body))
	logger.Info("Headers: %v", headers)
	logger.Info("Params: %v", params)
	return nil
}

func (s *LogSink) Close() error {
	return nil
}

func (s *LogSink) GetID() string {
	return s.ID
}

func (s *LogSink) GetExposedPort() (int, bool) {
	return 0, false
}
