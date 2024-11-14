package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
	"github.com/chamot1111/replayforge/pkgs/logger"
)

type RepeatFileSource struct {
	BaseSource
	FilePath    string
	EventChan   chan<- EventSource
	Interval    time.Duration
}

func (r *RepeatFileSource) Init(config SourceConfig, eventChan chan<- EventSource) error {
	var params struct {
		FilePath string `json:"filePath"`
		Interval int64  `json:"interval"`
	}
	if err := json.Unmarshal(config.Params, &params); err != nil {
		return fmt.Errorf("failed to parse RepeatFile source config: %v", err)
	}
	r.BaseSource = config.BaseSource
	r.FilePath = params.FilePath
	r.Interval = time.Duration(params.Interval) * time.Millisecond
	r.EventChan = eventChan
	return nil
}

func (r *RepeatFileSource) Start() error {
	go r.readLogFile()
	return nil
}

func (r *RepeatFileSource) Stop() error {
	// Implement graceful shutdown if needed
	return nil
}

func (r *RepeatFileSource) readLogFile() {
	for {
		file, err := os.Open(r.FilePath)
		if err != nil {
			logger.ErrorContext("source", r.ID, "Failed to open file %s: %v", r.FilePath, err)
			time.Sleep(r.Interval)
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			event := EventSource{
				SourceID: r.ID,
				Content:  line,
				Time:     time.Now(),
			}
			r.EventChan <- event
		}

		if err := scanner.Err(); err != nil {
			logger.ErrorContext("source", r.ID, "Error reading file %s: %v", r.FilePath, err)
		}

		file.Close()
		time.Sleep(r.Interval)
	}
}

// var Source RepeatFileSource
