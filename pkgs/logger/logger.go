package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
	messageExpirationSeconds = 3*60
	historyLength = 100
)

type logEntry struct {
	message   string
	timestamp time.Time
	level     LogLevel
	count     int
}

// ContextLogEntry stores context information for a log message
type ContextLogEntry struct {
	Message     string
	Timestamp   time.Time
	Level       LogLevel
	SinkOrSource string
	ID          string
}

var (
	currentLogLevel LogLevel
	mu             sync.Mutex
	logger         *log.Logger
	logHistory     map[LogLevel][]logEntry
	contextHistory map[string][]ContextLogEntry // key is "sinkOrSource:id"
	warnCount      int64
	errorCount     int64
)

// Configuration structure
type Config struct {
	Level      string    // "error", "warn", "info", "debug", "trace"
	TimeFormat string    // format de l'horodatage
	Output     *os.File  // destination des logs (par défaut os.Stdout)
}

func init() {
	logHistory = make(map[LogLevel][]logEntry)
	contextHistory = make(map[string][]ContextLogEntry)
	// Configuration par défaut
	Configure(Config{
		Level:      "info",
		TimeFormat: "2006-01-02 15:04:05",
		Output:     os.Stdout,
	})
}

// Configure permet de configurer le logger
func Configure(config Config) {
	mu.Lock()
	defer mu.Unlock()

	// Configuration du niveau de log
	switch strings.ToLower(config.Level) {
	case "trace":
		currentLogLevel = LogLevelTrace
	case "debug":
		currentLogLevel = LogLevelDebug
	case "info":
		currentLogLevel = LogLevelInfo
	case "warn":
		currentLogLevel = LogLevelWarn
	case "error":
		currentLogLevel = LogLevelError
	default:
		currentLogLevel = LogLevelInfo
	}

	// Configuration de la sortie
	output := config.Output
	if output == nil {
		output = os.Stdout
	}

	// Configuration du format de temps
	timeFormat := config.TimeFormat
	if timeFormat == "" {
		timeFormat = "2006-01-02 15:04:05"
	}

	// Initialisation du logger
	logger = log.New(output, "", 0)
}

// GetLogStats returns the count of warnings and errors
func GetLogStats() (int64, int64) {
	mu.Lock()
	defer mu.Unlock()
	return warnCount, errorCount
}

// GetContextHistory returns the last 10 messages for a given sink/source and id
func GetContextHistory(sinkOrSource string, id string) []ContextLogEntry {
	mu.Lock()
	defer mu.Unlock()
	key := fmt.Sprintf("%s:%s", sinkOrSource, id)
	return contextHistory[key]
}

// SetLogLevel permet de changer le niveau de log dynamiquement
func SetLogLevel(level string) {
	Configure(Config{Level: level})
	fmt.Printf("Logger level set to: %s\n", level)
}

func shouldLog(level LogLevel, msg string) (bool, string) {
	mu.Lock()
	defer mu.Unlock()

	entries := logHistory[level]
	now := time.Now()

	// Clean old messages and check if message exists in recent history
	var validEntries []logEntry
	for i := range entries {
        // Use pointer to modify the actual entry
        if now.Sub(entries[i].timestamp) < time.Duration(messageExpirationSeconds)*time.Second {
            if entries[i].message == msg {
                if entries[i].count == 1 {
                    minutesLeft := messageExpirationSeconds/60 - int(now.Sub(entries[i].timestamp).Minutes())
                    entries[i].count++
                    validEntries = append(validEntries, entries[i])
                    // Update the entry in the original map
                    logHistory[level] = validEntries
                    return true, fmt.Sprintf("%s (future occurrences will be muted for %d minutes)", msg, minutesLeft)
                }
                return false, ""
            }
            validEntries = append(validEntries, entries[i])
        }
    }

	// Add new entry
	newEntry := logEntry{message: msg, timestamp: now, count: 1}
	if len(validEntries) >= 5 {
		logHistory[level] = append(validEntries[1:], newEntry)
	} else {
		logHistory[level] = append(validEntries, newEntry)
	}

	return true, msg
}

func logMsg(level LogLevel, format string, v ...interface{}) {
	if level <= currentLogLevel {
		msg := fmt.Sprintf(format, v...)

		shouldDisplay, displayMsg := shouldLog(level, msg)
		if !shouldDisplay {
			return
		}

		mu.Lock()
		defer mu.Unlock()

		// Increment counters
		if level == LogLevelWarn {
			warnCount++
		} else if level == LogLevelError {
			errorCount++
		}

		var levelStr string
		switch level {
		case LogLevelTrace:
			levelStr = "TRACE"
		case LogLevelDebug:
			levelStr = "DEBUG"
		case LogLevelInfo:
			levelStr = "INFO "
		case LogLevelWarn:
			levelStr = "WARN "
		case LogLevelError:
			levelStr = "ERROR"
		}

		logger.Printf("[%s] %s | %s", time.Now().Format("2006-01-02 15:04:05"), levelStr, displayMsg)
	}
}

func logMsgWithContext(level LogLevel, sinkOrSource string, id string, format string, v ...interface{}) {
	if level <= currentLogLevel {
		msg := fmt.Sprintf(format, v...)
		contextMsg := fmt.Sprintf("[%s:%s] %s", sinkOrSource, id, msg)

		shouldDisplay, displayMsg := shouldLog(level, contextMsg)
		if !shouldDisplay {
			return
		}

		mu.Lock()
		defer mu.Unlock()

		// Store in context history
		key := fmt.Sprintf("%s:%s", sinkOrSource, id)
		entry := ContextLogEntry{
			Message: msg,
			Timestamp: time.Now(),
			Level: level,
			SinkOrSource: sinkOrSource,
			ID: id,
		}

		history := contextHistory[key]
		if len(history) >= historyLength {
			history = append(history[1:], entry)
		} else {
			history = append(history, entry)
		}
		contextHistory[key] = history

		if level == LogLevelWarn {
			warnCount++
		} else if level == LogLevelError {
			errorCount++
		}

		var levelStr string
		switch level {
		case LogLevelTrace:
			levelStr = "TRACE"
		case LogLevelDebug:
			levelStr = "DEBUG"
		case LogLevelInfo:
			levelStr = "INFO "
		case LogLevelWarn:
			levelStr = "WARN "
		case LogLevelError:
			levelStr = "ERROR"
		}

		logger.Printf("[%s] %s | %s", time.Now().Format("2006-01-02 15:04:05"), levelStr, displayMsg)
	}
}

// Original logging functions
func Trace(format string, v ...interface{}) {
	logMsg(LogLevelTrace, format, v...)
}

func Debug(format string, v ...interface{}) {
	logMsg(LogLevelDebug, format, v...)
}

func Info(format string, v ...interface{}) {
	logMsg(LogLevelInfo, format, v...)
}

func Warn(format string, v ...interface{}) {
	logMsg(LogLevelWarn, format, v...)
}

func Error(format string, v ...interface{}) {
	logMsg(LogLevelError, format, v...)
}

// Contextual logging functions
func TraceContext(sinkOrSource string, id string, format string, v ...interface{}) {
	logMsgWithContext(LogLevelTrace, sinkOrSource, id, format, v...)
}

func DebugContext(sinkOrSource string, id string, format string, v ...interface{}) {
	logMsgWithContext(LogLevelDebug, sinkOrSource, id, format, v...)
}

func InfoContext(sinkOrSource string, id string, format string, v ...interface{}) {
	logMsgWithContext(LogLevelInfo, sinkOrSource, id, format, v...)
}

func WarnContext(sinkOrSource string, id string, format string, v ...interface{}) {
	logMsgWithContext(LogLevelWarn, sinkOrSource, id, format, v...)
}

func ErrorContext(sinkOrSource string, id string, format string, v ...interface{}) {
	logMsgWithContext(LogLevelError, sinkOrSource, id, format, v...)
}

// Fatal log le message et termine le programme
func Fatal(format string, v ...interface{}) {
	logMsg(LogLevelError, format, v...)
	os.Exit(1)
}

func FatalContext(sinkOrSource string, id string, format string, v ...interface{}) {
	logMsgWithContext(LogLevelError, sinkOrSource, id, format, v...)
	os.Exit(1)
}

// IsLevelEnabled permet de vérifier si un niveau de log est activé
func IsLevelEnabled(level LogLevel) bool {
	return level <= currentLogLevel
}
