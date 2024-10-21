package main

import (
	"encoding/json"
	"fmt"
	"os"
	"github.com/chamot1111/replayforge/playerplugin"
	"log"
)

type LogSink struct {
	BucketName  string
	LogLevel    string
	LogFilePath string
	logger      *log.Logger
	logFile     *os.File
	ID          string
}

func (s *LogSink) Init(config playerplugin.SinkConfig) error {
	s.BucketName = config.Name

	var params map[string]interface{}
	if err := json.Unmarshal(config.Params, &params); err != nil {
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

	var err error
	s.logFile, err = os.OpenFile(s.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}

	s.logger = log.New(s.logFile, "", log.LstdFlags)
	s.ID = config.ID
	return nil
}

func (s *LogSink) Start() error {
	return nil
}

func (s *LogSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}) error {
	s.logger.Printf("Method: %s\n", method)
	s.logger.Printf("Path: %s\n", path)
	s.logger.Printf("Body: %s\n", string(body))
	s.logger.Printf("Headers: %v\n", headers)
	s.logger.Printf("Params: %v\n", params)
	return nil
}

func (s *LogSink) Close() error {
	if s.logFile != nil {
		return s.logFile.Close()
	}
	return nil
}

func (s *LogSink) GetID() string {
 return s.ID
}
