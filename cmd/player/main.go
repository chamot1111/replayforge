package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	_ "github.com/mattn/go-sqlite3"
)

var (
	configPath                string
	config                    map[string]interface{}
	proxyDbPath               = "player.sqlite3"
	dbFolder                  string
	heartbeatIntervalMs       = 100
	maxDbSize                 = 100 * 1024 * 1024 // 100 MB
	relayUrl                  string
	relayAuthenticationBearer string
	bucket                    string
	targetHost                string
	serverSecret              string
	db                        *sql.DB
	dbMode                    bool
	tableSchemas              map[string]map[string]string
	httpPort                  int
	httpUsername              string
	httpPassword              string
	staticAssetsFolder        string
)

func init() {
	flag.StringVar(&configPath, "c", "", "Path to config file")
	flag.StringVar(&relayUrl, "r", "", "Relay URL")
	flag.StringVar(&relayAuthenticationBearer, "a", "", "Relay authentication bearer")
	flag.StringVar(&bucket, "b", "", "Bucket")
	flag.StringVar(&targetHost, "t", "", "Target host")
	flag.StringVar(&serverSecret, "s", "", "Server secret")
	flag.BoolVar(&dbMode, "db", false, "Enable DB mode")
	flag.IntVar(&httpPort, "port", 8083, "HTTP server port")
	flag.Parse()
	if configPath != "" {
		fmt.Printf("Config path: %s\n", configPath)
		configData, err := os.ReadFile(configPath)
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
		if config["dbMode"] != nil {
			dbMode = config["dbMode"].(bool)
		}
		if config["httpPort"] != nil {
			httpPort = int(config["httpPort"].(float64))
		}
		if config["httpUsername"] != nil {
			httpUsername = config["httpUsername"].(string)
		}
		if config["httpPassword"] != nil {
			httpPassword = config["httpPassword"].(string)
		}
		if config["staticAssetsFolder"] != nil {
			staticAssetsFolder = config["staticAssetsFolder"].(string)
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
	if serverSecret == "" {
		panic("serverSecret must be specified either with -s flag or in the config file")
	}
	if httpUsername == "" || httpPassword == "" {
		panic("httpUsername and httpPassword must be specified in the config file")
	}

	if targetHost == "" {
		fmt.Println("No target host provided. Operating in DB mode only.")
		dbMode = true
	}

	dbFolder = filepath.Dir(proxyDbPath)

	if relayUrl != "" && !strings.HasSuffix(relayUrl, "/") {
		relayUrl += "/"
	}

	if targetHost != "" && !strings.HasSuffix(targetHost, "/") {
		targetHost += "/"
	}

	if dbMode {
		var err error
		db, err = sql.Open("sqlite3", proxyDbPath)
		if err != nil {
			panic(fmt.Sprintf("Failed to open database: %v", err))
		}
		err = db.Ping()
		if err != nil {
			panic(fmt.Sprintf("Failed to connect to database: %v", err))
		}
		fmt.Println("Connected to SQLite database")
		tableSchemas = make(map[string]map[string]string)
		loadTableSchemas()
	}
}

func loadTableSchemas() {
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		panic(fmt.Sprintf("Failed to query tables: %v", err))
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			panic(fmt.Sprintf("Failed to scan table name: %v", err))
		}

		tableInfo, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
		if err != nil {
			panic(fmt.Sprintf("Failed to get table info for %s: %v", tableName, err))
		}
		defer tableInfo.Close()

		tableSchemas[tableName] = make(map[string]string)
		for tableInfo.Next() {
			var cid int
			var name, dataType string
			var notNull, pk int
			var dfltValue interface{}
			if err := tableInfo.Scan(&cid, &name, &dataType, &notNull, &dfltValue, &pk); err != nil {
				panic(fmt.Sprintf("Failed to scan column info: %v", err))
			}
			tableSchemas[tableName][name] = dataType
		}
	}
}

func ensureTableExists(table string) error {
	if _, exists := tableSchemas[table]; !exists {
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ()", table)
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %v", table, err)
		}
		tableSchemas[table] = make(map[string]string)
	}
	return nil
}

func ensureColumnExists(table, column, dataType string) error {
	if _, exists := tableSchemas[table][column]; !exists {
		query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, dataType)
		if column == "id" {
			query += " PRIMARY KEY"
		}
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to add column %s to table %s: %v", column, table, err)
		}
		tableSchemas[table][column] = dataType

		if strings.HasPrefix(column, "_i") || strings.HasPrefix(column, "_idx") || strings.HasPrefix(column, "_index") || strings.HasPrefix(column, "id_") || strings.HasSuffix(column, "_id") || column == "id" {
			indexQuery := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s(%s)", table, column, table, column)
			_, err = db.Exec(indexQuery)
			if err != nil {
				return fmt.Errorf("failed to create index on column %s of table %s: %v", column, table, err)
			}
		}
	}
	return nil
}

func executeWriteDBOperation(method, path string, body []byte) error {
	if !strings.HasPrefix(path, "/rpf-db") {
		return nil
	}
	table := strings.TrimPrefix(path, "/rpf-db/")
	err := ensureTableExists(table)
	if err != nil {
		return err
	}

	switch method {
	case "POST":
		var data map[string]interface{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %v", err)
		}

		if _, ok := data["id"]; !ok {
			return fmt.Errorf("id is mandatory")
		}

		columns := make([]string, 0, len(data))
		values := make([]interface{}, 0, len(data))
		placeholders := make([]string, 0, len(data))
		updateStatements := make([]string, 0, len(data))
		for k, v := range data {
			columns = append(columns, k)
			values = append(values, v)
			placeholders = append(placeholders, "?")
			updateStatements = append(updateStatements, fmt.Sprintf("%s = ?", k))
			dataType := "TEXT"
			switch v.(type) {
			case int, int64:
				dataType = "INTEGER"
			case float64:
				dataType = "REAL"
			case bool:
				dataType = "BOOLEAN"
			}
			err = ensureColumnExists(table, k, dataType)
			if err != nil {
				return err
			}
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT(id) DO UPDATE SET %s",
			table,
			strings.Join(columns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(updateStatements, ", "))
		_, err = db.Exec(query, append(values, values...)...)
		if err != nil {
			return fmt.Errorf("failed to insert or update: %v", err)
		}
	case "PUT":
		var data map[string]interface{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %v", err)
		}
		id, ok := data["id"]
		if !ok {
			return fmt.Errorf("id is required for update")
		}
		delete(data, "id")
		setStatements := make([]string, 0, len(data))
		values := make([]interface{}, 0, len(data)+1)
		for k, v := range data {
			setStatements = append(setStatements, fmt.Sprintf("%s = ?", k))
			values = append(values, v)
			dataType := "TEXT"
			switch v.(type) {
			case int, int64:
				dataType = "INTEGER"
			case float64:
				dataType = "REAL"
			case bool:
				dataType = "BOOLEAN"
			}
			err = ensureColumnExists(table, k, dataType)
			if err != nil {
				return err
			}
		}
		values = append(values, id)
		query := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?",
			table,
			strings.Join(setStatements, ", "))
		_, err = db.Exec(query, values...)
		if err != nil {
			return fmt.Errorf("failed to update: %v", err)
		}
	case "DELETE":
		var data map[string]interface{}
		err := json.Unmarshal(body, &data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal JSON: %v", err)
		}
		id, ok := data["id"]
		if !ok {
			return fmt.Errorf("id is required for delete")
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", table)
		_, err = db.Exec(query, id)
		if err != nil {
			return fmt.Errorf("failed to delete: %v", err)
		}
	default:
		return fmt.Errorf("unsupported method: %s", method)
	}
	return nil
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
		ackBody, err := io.ReadAll(ackResp.Body)
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

	if dbMode {
		err := executeWriteDBOperation(method, path, []byte(requestBody))
		if err != nil {
			fmt.Printf("Error executing DB operation: %v\n", err)
		}
	}

	if targetHost != "" {
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
		_, err = io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Error reading response body from target host: %v\n", err)
			return
		}
	}
}

func handleGetRequest(w http.ResponseWriter, r *http.Request) {
	table := strings.TrimPrefix(r.URL.Path, "/")
	query := fmt.Sprintf("SELECT * FROM %s", table)

	// Parse query parameters
	whereClause := ""
	orderByClause := ""
	limitClause := ""

	for key, values := range r.URL.Query() {
		if key == "order" {
			orderByClause = fmt.Sprintf(" ORDER BY %s", values[0])
		} else if key == "limit" {
			limitClause = fmt.Sprintf(" LIMIT %s", values[0])
		} else {
			if whereClause == "" {
				whereClause = " WHERE "
			} else {
				whereClause += " AND "
			}
			whereClause += fmt.Sprintf("%s = ?", key)
			query += whereClause
		}
	}

	query += orderByClause + limitClause

	// Prepare and execute the query
	stmt, err := db.Prepare(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error preparing query: %v", err), http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	// Extract values for WHERE clause
	var queryArgs []interface{}
	for key, values := range r.URL.Query() {
		if key != "order" && key != "limit" {
			queryArgs = append(queryArgs, values[0])
		}
	}

	rows, err := stmt.Query(queryArgs...)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error querying table: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error get columns: %v", err), http.StatusInternalServerError)
		return
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}

		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		result = append(result, entry)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func basicAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		authParts := strings.SplitN(auth, " ", 2)
		if len(authParts) != 2 || authParts[0] != "Basic" {
			http.Error(w, "Invalid Authorization header", http.StatusBadRequest)
			return
		}

		payload, _ := base64.StdEncoding.DecodeString(authParts[1])
		pair := strings.SplitN(string(payload), ":", 2)

		if len(pair) != 2 || pair[0] != httpUsername || pair[1] != httpPassword {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func main() {
	if dbMode {
		defer db.Close()

		http.HandleFunc("/", basicAuth(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "GET" {
				handleGetRequest(w, r)
			} else {
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
		}))

		if staticAssetsFolder != "" {
			fs := http.FileServer(http.Dir(staticAssetsFolder))
			http.Handle("/assets/", http.StripPrefix("/assets/", fs))
		}

		fmt.Printf("Starting HTTP server on localhost:%d\n", httpPort)
		go func() {
			if err := http.ListenAndServe(fmt.Sprintf("localhost:%d", httpPort), nil); err != nil {
				fmt.Printf("Error starting HTTP server: %v\n", err)
			}
		}()
	}

	ticker := time.NewTicker(time.Duration(heartbeatIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		OnServerHeartbeat()
	}
}
