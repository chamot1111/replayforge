package main

import (
	"encoding/json"
	"github.com/chamot1111/replayforge/pkgs/logger"
	"time"
	"github.com/Shopify/go-lua"
	"github.com/chamot1111/replayforge/pkgs/lualibs"
	"fmt"
	"os"
)

func processEvent(event EventSource) {
	vm, ok := vms[event.SourceID]
	if !ok {
		logger.Warn("VM not found for source %s", event.SourceID)
		return
	}

	vm.Global("Process")
	if !vm.IsFunction(-1) {
		logger.Warn("process function not found in script for source %s", event.SourceID)
		vm.Pop(1)
		return
	}

	vm.PushString(event.Content)
	vm.PushGoFunction(func(l *lua.State) int {
		sinkID, _ := l.ToString(-2)
		emittedContent, _ := l.ToString(-1)
		if ch, ok := sinkChannels[sinkID]; ok {
			stats.Lock()
			sinkStats := stats.Sinks[sinkID]
			sinkStats.MessagesByMinute++
			sinkStats.MessagesSinceStart++
			sinkStats.LastMessageDate = time.Now()
			stats.Sinks[sinkID] = sinkStats
			stats.Unlock()
			ch <- emittedContent
		} else {
			logger.Warn("Sink channel not found for sink %s", sinkID)
		}
		return 0
	})

	if err := vm.ProtectedCall(2, 0, 0); err != nil {
		logger.Error("Failed to run process function for source %s: %v", event.SourceID, err)
	}
}


func timerHandler(sourceID string) {
	vm, ok := vms[sourceID]
	if !ok {
		return
	}

	vm.Global("TimerHandler")
	if !vm.IsFunction(-1) {
		vm.Pop(1)
		return
	}

	vm.PushGoFunction(func(l *lua.State) int {
		sinkID, _ := l.ToString(-2)
		emittedContent, _ := l.ToString(-1)
		if ch, ok := sinkChannels[sinkID]; ok {
			ch <- emittedContent
		} else {
			logger.Warn("Sink channel not found for sink %s", sinkID)
		}
		return 0
	})

	if err := vm.ProtectedCall(1, 0, 0); err != nil {
		logger.Error("Failed to run timer_handler function for source %s: %v", sourceID, err)
	}
}


func setupSources() {
	for _, rawSource := range config.Sources {
		var sourceConfig SourceConfig
		if err := json.Unmarshal(rawSource, &sourceConfig); err != nil {
			logger.Fatal("Failed to parse source JSON: %v", err)
		}

		var source Source
		switch sourceConfig.Type {
		case "http":
			source = &HTTPSource{}
		case "logfile":
			source = &LogFileSource{}
		case "repeatfile":
			source = &RepeatFileSource{}
		case "pgcall":
			source = &PgCallSource{}
		default:
			logger.Fatal("Unsupported source type: %s", sourceConfig.Type)
		}

		eventChan := make(chan EventSource)
		if err := source.Init(sourceConfig, eventChan); err != nil {
			logger.Fatal("Failed to initialize source %s: %v", sourceConfig.ID, err)
		}

		sources[sourceConfig.ID] = source

		stats.Lock()
		stats.Sources[sourceConfig.ID] = SourceStats{
			Type: sourceConfig.Type,
			ID:   sourceConfig.ID,
		}
		stats.Unlock()

		go func(s Source, ch <-chan EventSource) {
			for event := range ch {
				stats.Lock()
				sourceStats := stats.Sources[event.SourceID]
				sourceStats.MessagesByMinute++
				sourceStats.MessagesSinceStart++
				sourceStats.LastMessageDate = time.Now()
				stats.Sources[event.SourceID] = sourceStats
				stats.Unlock()
				processEvent(event)
			}
		}(source, eventChan)

		setupSourceVM(sourceConfig)
	}
}

func setupSourceVM(sourceConfig SourceConfig) {
	vm := lua.NewState()
	lua.OpenLibraries(vm)
	lualibs.RegisterLuaLibs(vm)

	if sourceConfig.TransformScript != "" {
		script, err := os.ReadFile(sourceConfig.TransformScript)
		if err != nil {
			logger.Fatal("Failed to read script file for source %s: %v", sourceConfig.ID, err)
		}
		if err := lua.DoString(vm, string(script)); err != nil {
			logger.Fatal("Failed to load script for source %s: %v", sourceConfig.ID, err)
		}
	} else if sourceConfig.TargetSink != "" {
		defaultScript := fmt.Sprintf(`
			function Process(event, emit)
				emit("%s", event)
			end
		`, sourceConfig.TargetSink)
		if err := lua.DoString(vm, defaultScript); err != nil {
			logger.Fatal("Failed to load default script for source %s: %v", sourceConfig.ID, err)
		}
	} else {
		logger.Fatal("Either TransformScript or TargetSink must be provided for source %s", sourceConfig.ID)
	}

	vm.Global("init")
	if vm.IsFunction(-1) {
		if err := vm.ProtectedCall(0, 0, 0); err != nil {
			logger.Fatal("Failed to run init hook for source %s: %v", sourceConfig.ID, err)
		}
	}
	vm.Pop(1)

	vms[sourceConfig.ID] = vm
}
