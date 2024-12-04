package main

import (
	"encoding/json"
	"time"
	_ "github.com/mattn/go-sqlite3"

	"github.com/chamot1111/replayforge/pkgs/logger"
	"github.com/chamot1111/replayforge/internal/envparser"
	"strings"
	"os"
	"github.com/Shopify/go-lua"
)

type SinkConfig struct {
	MaxPayloadBytes  int `json:"maxPayloadBytes"`  // Max payload size in bytes
	BatchGoalBytes   int `json:"batchGoalBytes"`   // Target batch size in bytes
	BatchMaxEvents   int `json:"batchMaxEvents"`   // Max events per batch
	BatchTimeoutSecs int `json:"batchTimeoutSecs"` // Batch timeout in seconds
}


type BaseSource struct {
	ID              string      `json:"id"`
	Type            string      `json:"type"`
	TransformScript string      `json:"transformScript"`
	TargetSink      string      `json:"targetSink"`
	HookInterval    interface{} `json:"hookInterval"`
}

type SourceConfig struct {
	BaseSource
	Params json.RawMessage `json:"params"`
}

type Config struct {
	Sources         []json.RawMessage
	Sinks           []Sink
	TsnetHostname   string `json:"tsnetHostname"`
	PortStatusZ     int    `json:"portStatusZ"`
	EnvName         string `json:"envName"`
	HostName        string `json:"hostName"`
	UseTsnetStatusZ bool   `json:"useTsnetStatusZ"`
	EnablePprof     bool   `json:"enablePprof"`   // Enable pprof profiling
	VerbosityLevel  string `json:"verbosityLevel"` // Optional verbosity level (debug, info, warn, error)
}


func (bs *BaseSource) GetHookInterval() time.Duration {
	switch v := bs.HookInterval.(type) {
	case string:
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
		logger.ErrorContext("source", bs.ID, "Invalid duration string for HookInterval: %s (%v)", v, err)
		return 0
	case float64:
		return time.Duration(v) * time.Second
	case int:
		return time.Duration(v) * time.Second
	default:
		return 0
	}
}

func loadConfig() {
	var configData []byte
	var err error

	if dbgScriptPath != "" {
		configData = []byte(strings.Replace(strings.Replace(debugConfig, "${script_path}", dbgScriptPath, -1), "${file_path}", dbgFilePath, -1))
	} else {
		logger.Info("Config path: %s", configPath)
		configData, err = os.ReadFile(configPath)
		if err != nil {
			logger.Fatal("Failed to read config file: %v", err)
		}
	}

	configDataStr, err := envparser.ProcessJSONWithEnvVars(string(configData))
	if err != nil {
		logger.Fatal("Failed to process config file with environment variables: %v", err)
	}
	configData = []byte(configDataStr)

	if err := json.Unmarshal(configData, &config); err != nil {
		logger.Fatal("Failed to parse config JSON: %v", err)
	}

	if config.UseTsnetStatusZ && config.TsnetHostname == "" {
		logger.Fatal("TsnetHostname must be set when UseTsnetStatusZ is true")
	}

	if config.EnablePprof && config.PortStatusZ == 0 {
		logger.Fatal("PortStatusZ must be set when EnablePprof is true")
	}

	if config.HostName == "" {
		hostname, err := os.Hostname()
		if err == nil {
			config.HostName = hostname
		}
	}

	vms = make(map[string]*lua.State)
	lastVacuumTimes = make(map[string]time.Time)
	sources = make(map[string]Source)
	sinkChannels = make(map[string]chan string)
}
