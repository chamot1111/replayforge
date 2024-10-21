package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"github.com/chamot1111/replayforge/playerplugin"
)

type HttpSink struct {
	TargetHost string
	BucketName string
	ID string
}

func (s *HttpSink) Init(config playerplugin.SinkConfig) error {
	var params struct {
		TargetHost string `json:"targetHost"`
	}
	err := json.Unmarshal(config.Params, &params)
	if err != nil {
		return fmt.Errorf("failed to parse sink params: %v", err)
	}
	s.TargetHost = params.TargetHost
	s.BucketName = config.Name
	s.ID = config.ID
	return nil
}

func (s *HttpSink) Start() error {
	return nil
}

func (s *HttpSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}) error {
	targetUrl := s.TargetHost + path
	client := &http.Client{}

	req, err := http.NewRequest(method, targetUrl, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("error creating request for target host: %v", err)
	}

	for key, value := range headers {
		req.Header.Set(key, fmt.Sprint(value))
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error fetching from target host: %v", err)
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body from target host: %v", err)
	}

	return nil
}

func (s *HttpSink) Close() error {
	// No need to close anything for HttpSink
	return nil
}

func (s *HttpSink) GetID() string {
 return s.ID
}
