package main

import (
 "plugin"
 "github.com/chamot1111/replayforge/playerplugin"
 "fmt"
)

func LoadSinkPlugin(path string) (playerplugin.Sink, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}

	symSink, err := p.Lookup("SinkInstance")
	if err != nil {
		return nil, err
	}

	sinkPlugin, ok := symSink.(playerplugin.SinkPlugin)
	if !ok {
		return nil, fmt.Errorf("unexpected type from module symbol")
	}

	return sinkPlugin.NewSink()
}
