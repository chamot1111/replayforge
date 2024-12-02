package main

import (
 "encoding/json"
 "fmt"
)

func BuildProxyCallObject(path, method string, body []byte, params, headers map[string]interface{}) ([]byte, error) {
 wrapCallObject := map[string]interface{}{
  "path":    path,
  "params":  params,
  "headers": headers,
  "body":    string(body),
  "method":  method,
 }

 wrapJsonContent, err := json.Marshal(wrapCallObject)
 if err != nil {
  return nil, fmt.Errorf("error marshaling proxy call object: %v", err)
 }

 return wrapJsonContent, nil
}
