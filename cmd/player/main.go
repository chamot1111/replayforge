package main

import (
 "encoding/json"
 "flag"
 "fmt"
 "io/ioutil"
 "net/http"
 "net/url"
 "path/filepath"
 "strings"
 "time"

 "github.com/golang-jwt/jwt/v5"
)

var (
 configPath              string
 config                  map[string]interface{}
 proxyDbPath             = "player.sqlite3"
 dbFolder                string
 heartbeatIntervalMs     = 100
 maxDbSize               = 100 * 1024 * 1024 // 100 MB
 relayUrl                string
 relayAuthenticationBearer string
 bucket                  string
 targetHost              string
 serverSecret            string
)

func init() {
 flag.StringVar(&configPath, "c", "", "Path to config file")
 flag.StringVar(&relayUrl, "r", "", "Relay URL")
 flag.StringVar(&relayAuthenticationBearer, "a", "", "Relay authentication bearer")
 flag.StringVar(&bucket, "b", "", "Bucket")
 flag.StringVar(&targetHost, "t", "", "Target host")
 flag.StringVar(&serverSecret, "s", "", "Server secret")
 flag.Parse()

 if configPath != "" {
  fmt.Printf("Config path: %s\n", configPath)
  configData, err := ioutil.ReadFile(configPath)
  if err != nil {
   panic(err)
  }
  err = json.Unmarshal(configData, &config)
  if err != nil {
   panic(err)
  }
  if config["relayUrl"] != nil {
   relayUrl = config["relayUrl"].(string)
  }
  if config["relayAuthenticationBearer"] != nil {
   relayAuthenticationBearer = config["relayAuthenticationBearer"].(string)
  }
  if config["bucket"] != nil {
   bucket = config["bucket"].(string)
  }
  if config["targetHost"] != nil {
   targetHost = config["targetHost"].(string)
  }
  if config["serverSecret"] != nil {
   serverSecret = config["serverSecret"].(string)
  }
 }

 if relayUrl == "" {
  panic("relayUrl must be specified either with -r flag or in the config file")
 }

 if relayAuthenticationBearer == "" {
  panic("relayAuthenticationBearer must be specified either with -a flag or in the config file")
 }

 if bucket == "" {
  panic("bucket must be specified either with -b flag or in the config file")
 }

 if targetHost == "" {
  panic("targetHost must be specified either with -t flag or in the config file")
 }

 if serverSecret == "" {
  panic("serverSecret must be specified either with -s flag or in the config file")
 }

 dbFolder = filepath.Dir(proxyDbPath)

 if !strings.HasSuffix(relayUrl, "/") {
  relayUrl += "/"
 }

 if !strings.HasSuffix(targetHost, "/") {
  targetHost += "/"
 }
}

func OnServerHeartbeat() {
 client := &http.Client{}
 req, err := http.NewRequest("GET", relayUrl+"first", nil)
 if err != nil {
  fmt.Printf("Error creating request: %v\n", err)
  return
 }

 req.Header.Set("RF-BUCKET", bucket)
 req.Header.Set("Authorization", "Bearer "+relayAuthenticationBearer)

 resp, err := client.Do(req)
 if err != nil {
  fmt.Printf("Error fetching from relay: %v\n", err)
  return
 }
 defer resp.Body.Close()

 if resp.StatusCode == 404 {
  return
 }
 if resp.StatusCode != 200 {
  fmt.Printf("Unexpected status code: %d\n", resp.StatusCode)
  return
 }

 var responseMap map[string]interface{}
 err = json.NewDecoder(resp.Body).Decode(&responseMap)
 if err != nil {
  fmt.Printf("Error decoding response body: %v\n", err)
  return
 }

 id, ok := responseMap["id"].(float64)
 if !ok {
  fmt.Println("Error: 'id' not found in response or not a number")
  return
 }
 idInt := int(id)

 content, ok := responseMap["content"].(string)
 if !ok {
  fmt.Println("Error: 'content' not found in response or not a string")
  return
 }

 body := []byte(content)

 // Acknowledge relay server
 acknowledgeUrl := fmt.Sprintf("%sacknowledge?id=%d", relayUrl, idInt)
 ackReq, err := http.NewRequest("DELETE", acknowledgeUrl, nil)
 if err != nil {
  fmt.Printf("Error creating acknowledgment request: %v\n", err)
  return
 }
 ackReq.Header.Set("RF-BUCKET", bucket)
 ackReq.Header.Set("Authorization", "Bearer "+relayAuthenticationBearer)

 ackResp, err := client.Do(ackReq)
 if err != nil {
  fmt.Printf("Error sending acknowledgment: %v\n", err)
  return
 }
 defer ackResp.Body.Close()

 if ackResp.StatusCode != 200 {
  fmt.Printf("Unexpected status code from acknowledgment: %d\n", ackResp.StatusCode)
  ackBody, err := ioutil.ReadAll(ackResp.Body)
  if err != nil {
   fmt.Printf("Error reading acknowledgment response body: %v\n", err)
   return
  }
  fmt.Printf("Acknowledgment response: %s\n", string(ackBody))
  return
 }

 token, err := jwt.Parse(string(body), func(token *jwt.Token) (interface{}, error) {
  if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
   return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
  }
  return []byte(serverSecret), nil
 })

 if err != nil {
  fmt.Printf("Error decoding JWT: %v\n", err)
  return
 }

 claims, ok := token.Claims.(jwt.MapClaims)
 if !ok || !token.Valid {
  fmt.Println("Invalid JWT payload")
  return
 }

 wrapCallStr, ok := claims["data"].(string)
 if !ok {
  fmt.Println("Invalid JWT payload")
  return
 }

 var decodedData map[string]interface{}
 err = json.Unmarshal([]byte(wrapCallStr), &decodedData)
 if err != nil {
  fmt.Printf("Error decoding JSON data: %v\n", err)
  return
 }

 path, _ := decodedData["path"].(string)
 params, _ := decodedData["params"].(map[string]interface{})
 headers, _ := decodedData["headers"].(map[string]interface{})
 method, _ := decodedData["method"].(string)
 requestBody, _ := decodedData["body"].(string)

 targetUrl := targetHost + path
 if len(params) > 0 {
  values := url.Values{}
  for key, value := range params {
   values.Add(key, fmt.Sprint(value))
  }
  targetUrl += "?" + values.Encode()
 }

 req, err = http.NewRequest(method, targetUrl, strings.NewReader(requestBody))
 if err != nil {
  fmt.Printf("Error creating request for target host: %v\n", err)
  return
 }

 for key, value := range headers {
  req.Header.Set(key, fmt.Sprint(value))
 }

 resp, err = client.Do(req)
 if err != nil {
  fmt.Printf("Error fetching from target host: %v\n", err)
  return
 }
 defer resp.Body.Close()

 // Handle the response from the target host here
 // You might want to read the response body and do something with it
 _, err = ioutil.ReadAll(resp.Body)
 if err != nil {
  fmt.Printf("Error reading response body from target host: %v\n", err)
  return
 }
}

func main() {
 ticker := time.NewTicker(time.Duration(heartbeatIntervalMs) * time.Millisecond)
 defer ticker.Stop()

 for range ticker.C {
  OnServerHeartbeat()
 }
}
