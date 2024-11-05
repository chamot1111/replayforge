package main

import (
	"plugin"
	"github.com/chamot1111/replayforge/playerplugin"
	"github.com/chamot1111/replayforge/pkgs/logger"
)

func LoadSinkPlugin(path string) (playerplugin.Sink, error) {
	p, err := plugin.Open(path)
	if err != nil {
		logger.Error("Erreur: %v", err)
		return nil, err
	}

	symSink, err := p.Lookup("SinkInstance")
	if err != nil {
		logger.Error("Erreur: %v", err)
		return nil, err
	}

	sinkPlugin, ok := symSink.(playerplugin.SinkPlugin)
	if !ok {
		logger.Error("Erreur: %v", fmt.Errorf("unexpected type from module symbol"))
		return nil, fmt.Errorf("unexpected type from module symbol")
	}

	return sinkPlugin.NewSink()
}
