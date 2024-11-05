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
)

var (
	currentLogLevel LogLevel
	mu             sync.Mutex
	logger         *log.Logger
)

// Configuration structure
type Config struct {
	Level      string    // "error", "warn", "info", "debug", "trace"
	TimeFormat string    // format de l'horodatage
	Output     *os.File  // destination des logs (par défaut os.Stdout)
}

func init() {
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

// SetLogLevel permet de changer le niveau de log dynamiquement
func SetLogLevel(level string) {
	Configure(Config{Level: level})
}

func logMsg(level LogLevel, format string, v ...interface{}) {
	if level <= currentLogLevel {
		mu.Lock()
		defer mu.Unlock()

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

		msg := fmt.Sprintf(format, v...)
		logger.Printf("[%s] %s | %s", time.Now().Format("2006-01-02 15:04:05"), levelStr, msg)
	}
}

// Fonctions publiques pour chaque niveau de log
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

// Fatal log le message et termine le programme
func Fatal(format string, v ...interface{}) {
	logMsg(LogLevelError, format, v...)
	os.Exit(1)
}

// IsLevelEnabled permet de vérifier si un niveau de log est activé
func IsLevelEnabled(level LogLevel) bool {
	return level <= currentLogLevel
}
