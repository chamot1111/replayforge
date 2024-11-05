package main

import (
 "github.com/chamot1111/replayforge/pkgs/logger"
 "plugin"
 "time"
 "fmt"
)

type EventSource struct {
 Content     string
 SourceID string
 Time        time.Time
}

type Source interface {
 Init(config SourceConfig, eventChan chan<- EventSource) error
 Start() error
 Stop() error
}

func LoadSourcePlugin(path string) (Source, error) {
 p, err := plugin.Open(path)
 if err != nil {
  logger.Error("Erreur: %v", err)
  return nil, fmt.Errorf("failed to open plugin: %v", err)
 }

 symSource, err := p.Lookup("Source")
 if err != nil {
  logger.Error("Erreur: %v", err)
  return nil, fmt.Errorf("failed to lookup 'Source' symbol: %v", err)
 }

 source, ok := symSource.(Source)
 if !ok {
  logger.Error("Erreur: unexpected type from module symbol")
  return nil, fmt.Errorf("unexpected type from module symbol")
 }

 return source, nil
}
