package main

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
	"github.com/chamot1111/replayforge/pkgs/logger"
)

type LogFileSource struct {
	BaseSource
	FilePath         string
	EventChan        chan<- EventSource
	RemoveAfterSecs  uint
	FingerprintLines uint
	lastPosition     int64
	HttpPath         string
	lastTruncate     time.Time
	lastChecksum       string
}

func (l *LogFileSource) Init(config SourceConfig, eventChan chan<- EventSource) error {
	var params struct {
		FilePath         string `json:"filePath"`
		RemoveAfterSecs  uint   `json:"removeAfterSecs,omitempty"`
		FingerprintLines uint   `json:"fingerprintLines,omitempty"`
		HttpPath         string `json:"httpPath,omitempty"`
	}
	if err := json.Unmarshal(config.Params, &params); err != nil {
		return fmt.Errorf("failed to parse LogFile source config: %v", err)
	}
	l.BaseSource = config.BaseSource
	l.FilePath = params.FilePath
	l.RemoveAfterSecs = params.RemoveAfterSecs
	l.FingerprintLines = params.FingerprintLines
	l.EventChan = eventChan
	l.HttpPath = params.HttpPath
	if l.HttpPath == "" {
		l.HttpPath = "/"
	}
	l.lastTruncate = time.Now()
	return nil
}

func (l *LogFileSource) Start() error {
	l.lastPosition = 0
	go func() {
		for {
			l.readLogFile()
			time.Sleep(time.Duration(l.HookInterval) * time.Millisecond)
		}
	}()
	return nil
}

func (l *LogFileSource) Stop() error {
	// Implement graceful shutdown if needed
	return nil
}
func (l *LogFileSource) readLogFile() {
	file, err := os.Open(l.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			time.Sleep(time.Duration(l.HookInterval) * time.Millisecond)
			return
		}
		logger.Error("Failed to open log file %s: %v", l.FilePath, err)
		time.Sleep(time.Duration(l.HookInterval) * time.Millisecond)
		return
	}
	defer file.Close()

	checksum, enoughLines, err := l.calculateChecksum(file)
	if err != nil {
		logger.Error("Failed to calculate checksum for %s: %v", l.FilePath, err)
		time.Sleep(time.Duration(l.HookInterval) * time.Millisecond)
		return
	}

	if !enoughLines {
		logger.Debug("Not enough lines to calculate checksum for %s", l.FilePath)
		return
	}

	// If checksum is different, file has changed so start from beginning
	if checksum != l.lastChecksum {
		l.lastPosition = 0
		l.lastChecksum = checksum
	}

	if _, err = file.Seek(l.lastPosition, 0); err != nil {
		logger.Error("Failed to seek to last position in file %s: %v", l.FilePath, err)
		return
	}

	scanner := bufio.NewScanner(file)
	skippedEvent := false
	for scanner.Scan() {
		line := scanner.Text()
		// Process the line here
		logger.Debug("Read line: %s", line)
		bodyJSON, err := json.Marshal(map[string]string{"content": line})
		if err != nil {
			logger.Error("Error marshaling body JSON: %v", err)
			continue
		}

		wrapCallObject := map[string]interface{}{
			"ip":      "127.0.0.1",
			"path":    l.HttpPath,
			"params":  map[string]string{},
			"headers": map[string]string{"Content-Type": "application/json"},
			"body":    string(bodyJSON),
			"method":  "POST",
		}

		jsonContent, err := json.Marshal(wrapCallObject)
		if err != nil {
			logger.Error("Error marshaling JSON: %v", err)
			return
		}

		event := EventSource{
			SourceID: l.ID,
			Content:  string(jsonContent),
			Time:     time.Now(),
		}

		if !skippedEvent {
			select {
			case l.EventChan <- event:
				skippedEvent = false
			case <-time.After(100 * time.Millisecond):
				logger.Warn("EventChan is full, skipping all next events")
				skippedEvent = true
			}
		} else {
			select {
			case l.EventChan <- event:
				skippedEvent = false
			default:
				// Do nothing, continue to next event
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("Error reading log file %s: %v", l.FilePath, err)
	}

	l.lastPosition, _ = file.Seek(0, 1) // Get current position

	if l.RemoveAfterSecs > 0 && time.Since(l.lastTruncate) >= time.Duration(l.RemoveAfterSecs)*time.Second {
		logger.Info("Attempting to truncate log file %s", l.FilePath)
		if err := os.Truncate(l.FilePath, 0); err != nil {
			logger.Error("Failed to truncate log file %s: %v", l.FilePath, err)
		} else {
			logger.Info("Successfully truncated log file %s after %d seconds", l.FilePath, l.RemoveAfterSecs)
			l.lastTruncate = time.Now()
		}
	}
}

func (l *LogFileSource) calculateChecksum(file *os.File) (string, bool, error) {
	hash := md5.New()
	scanner := bufio.NewScanner(file)
	lineCount := 0

	for scanner.Scan() && lineCount < int(l.FingerprintLines) {
		_, err := io.WriteString(hash, scanner.Text())
		if err != nil {
			return "", false, err
		}
		lineCount++
	}

	if lineCount < int(l.FingerprintLines) {
		return "", false, fmt.Errorf("file has less than %d lines", l.FingerprintLines)
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), true, nil
}
