package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"net/http"
	"io"
	"github.com/chamot1111/replayforge/playerplugin"
	"github.com/chamot1111/replayforge/pkgs/logger"

	_ "github.com/mattn/go-sqlite3"
)

type WebhookConfig struct {
	URL         string `json:"url"`
	Header      map[string]string
	ContentType string `json:"contentType,omitempty,enum=plain|json"`
	Method      string `json:"method,omitempty,enum=GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS"`
	BuildPayload   string `json:"buildPayload,omitempty"`
}

type NotificationSink struct {
	ID             string
	WebhookConfigs map[string]WebhookConfig
}

func (s *NotificationSink) Init(config playerplugin.SinkConfig) error {
	// Initialize the NotificationSink with the provided configuration
	var params map[string]interface{}
	err := json.Unmarshal(config.Params, &params)
	if err != nil {
		return fmt.Errorf("failed to parse params: %v", err)
	}
	// Parse webhook configs
	webhooks, ok := params["webhooks"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("webhooks config not found or invalid")
	}

	s.WebhookConfigs = make(map[string]WebhookConfig)
	for name, cfg := range webhooks {
		webhookMap, ok := cfg.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid webhook config for %s", name)
		}

		url, ok := webhookMap["url"].(string)
		if !ok {
			return fmt.Errorf("URL missing for webhook %s", name)
		}

		headers, _ := webhookMap["header"].(map[string]string)
		contentType, _ := webhookMap["contentType"].(string)
		method, ok := webhookMap["method"].(string)
		if ok && method != "" {
			if method != "GET" && method != "POST" && method != "PUT" && method != "DELETE" && method != "PATCH" && method != "HEAD" && method != "OPTIONS" {
				method = "POST"
			}
		} else {
			method = "POST"
		}
		buildPayload, _ := webhookMap["buildPayload"].(string)

		s.WebhookConfigs[name] = WebhookConfig{
			URL:          url,
			Header:       headers,
			ContentType:  contentType,
			Method:       method,
			BuildPayload: buildPayload,
		}
	}

	s.ID = config.ID

	// Display the config to console
	logger.Info("NotificationSink Configuration:")
	logger.Info("ID: %s", s.ID)
	logger.Info("Webhook Configs:")
	for name, webhook := range s.WebhookConfigs {
		logger.Info("  %s:", name)
		logger.Info("    URL: %s", webhook.URL)
		logger.Info("    Method: %s", webhook.Method)
		logger.Info("    Content-Type: %s", webhook.ContentType)
		logger.Info("    Headers: %v", webhook.Header)
	}

	return nil
}

func (s *NotificationSink) Start() error {
	return nil
}

func (s *NotificationSink) Execute(method, path string, body []byte, headers map[string]interface{}, params map[string]interface{}, sinkChannels map[string]chan string) error {
	switch method {
	case "POST":
		return s.handlePost(body)
	default:
		return fmt.Errorf("unsupported method: %s", method)
	}
}

func (s *NotificationSink) Close() error {
	return nil
}

func (s *NotificationSink) handlePost(body []byte) error {
	var subject string
	var message string

	var data map[string]interface{}
	err := json.Unmarshal(body, &data)
	if err != nil {
		subject = ""
		message = string(body)
	} else {
		subject = ""
		if subj, ok := data["subject"].(string); ok {
			subject = subj
		}

		msg, ok := data["message"].(string)
		if !ok {
			return fmt.Errorf("message not found in JSON")
		}
		message = msg
	}

	var webhookErrors []error
	for webhookName, webhook := range s.WebhookConfigs {
		var payload string

		if webhook.BuildPayload == "" {
			payload = message
		} else {
			// Escape special characters that might break JSON
			escapedSubject := strings.ReplaceAll(subject, "\"", "\\\"")
			escapedSubject = strings.ReplaceAll(escapedSubject, "\n", "\\n")
			escapedSubject = strings.ReplaceAll(escapedSubject, "\r", "\\r")

			escapedMessage := strings.ReplaceAll(message, "\"", "\\\"")
			escapedMessage = strings.ReplaceAll(escapedMessage, "\n", "\\n")
			escapedMessage = strings.ReplaceAll(escapedMessage, "\r", "\\r")

			payload = strings.ReplaceAll(webhook.BuildPayload, "{{subject}}", escapedSubject)
			payload = strings.ReplaceAll(payload, "{{message}}", escapedMessage)
		}

		// Send the payload to the webhook
		if err := s.sendWebhook(webhookName, webhook, payload); err != nil {
			webhookErrors = append(webhookErrors, fmt.Errorf("failed to send webhook %s: %v", webhookName, err))
		}
	}

	if len(webhookErrors) > 0 {
		var errMsgs []string
		for _, err := range webhookErrors {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("webhook errors: %s", strings.Join(errMsgs, "; "))
	}

	return nil
}

func (s *NotificationSink) sendWebhook(webhookName string, webhook WebhookConfig, payload string) error {
	client := &http.Client{}
	method := webhook.Method
	if method == "" {
		method = "POST"
	}

	req, err := http.NewRequest(method, webhook.URL, strings.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	if webhook.ContentType != "" {
	contentType := "text/plain"
	if webhook.ContentType == "json" {
			contentType = "application/json"
	} else if webhook.ContentType == "plain" {
			contentType = "text/plain"
	}
	req.Header.Set("Content-Type", contentType)
	} else {
			req.Header.Set("Content-Type", "text/plain")
	}
	// Debug output for Content-Type header
	logger.Debug("Setting Content-Type header for webhook %s to: %s (Content-Type: %s)", webhookName, req.Header.Get("Content-Type"), webhook.ContentType)

	for key, value := range webhook.Header {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
			return fmt.Errorf("failed to read response body: %v", err)
	}
	return fmt.Errorf("webhook request failed with status code %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (s *NotificationSink) GetID() string {
	return s.ID
}

func (s *NotificationSink) GetExposedPort() (int, bool) {
	return 0, false
