package main

import (
 "database/sql"
 "fmt"
 "os"

 _ "github.com/mattn/go-sqlite3"
)


func setupSql(dbPath string, canVacuum bool) (*sql.DB, error, bool) {
	info, err := os.Stat(dbPath)
	var errDbSize error
	if err == nil && info.Size() > maxDbSize {
		errDbSize = fmt.Errorf("database size (%d bytes) exceeded maximum limit (%d bytes)", info.Size(), maxDbSize)
	}

	db, err := sql.Open("sqlite3", dbPath+"?_auto_vacuum=2&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err), false
	}

	if errDbSize != nil {
		return db, errDbSize, true
	}

	return db, nil, false
}

func initSetupSql(dbPath string, isSource bool) (*sql.DB, error, bool) {
	db, err, isSpaceError := setupSql(dbPath, true)
	if err != nil && !isSpaceError {
		if db != nil {
			db.Close()
		}
		return nil, fmt.Errorf("failed to setup database: %v", err), isSpaceError
	}

	if isSource {
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS source_events (
				id INTEGER PRIMARY KEY,
				content TEXT
			)
		`)
		if err != nil {
			return nil, fmt.Errorf("failed to create source_events table: %v", err), false
		}
	} else {
		_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS sink_events (
				id INTEGER PRIMARY KEY,
				content TEXT
			)
		`)
		if err != nil {
			return nil, fmt.Errorf("failed to create sink_events table: %v", err), false
		}
	}

	if isSpaceError {
		return db, fmt.Errorf("failed to setup database due to space constraints: %v", err), isSpaceError
	}
	return db, nil, false
}
