package main

import (
 "strconv"
 "time"
)

// normalizeTimestamp converts various timestamp formats to Unix seconds
func normalizeTimestamp(rawTimestamp interface{}) int64 {
	if rawTimestamp == nil {
		return time.Now().Unix()
	}

	switch v := rawTimestamp.(type) {
	case int64:
		return normalizeNumericTimestamp(v)
	case float64:
		return normalizeNumericTimestamp(int64(v))
	case string:
		return normalizeStringTimestamp(v)
	default:
		return time.Now().Unix()
	}
}

// normalizeNumericTimestamp converts numeric timestamps to Unix seconds
func normalizeNumericTimestamp(timestamp int64) int64 {
	if timestamp > 3000000000 {
		if timestamp > 1000000000000000 {
			return timestamp / 1000000000 // nanoseconds to seconds
		} else if timestamp > 1000000000000 {
			return timestamp / 1000000 // microseconds to seconds
		} else {
			return timestamp / 1000 // milliseconds to seconds
		}
	}
	return timestamp // already in seconds
}

// normalizeStringTimestamp converts string timestamps to Unix seconds
func normalizeStringTimestamp(timestamp string) int64 {
	// Try parsing common datetime formats
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"02/Jan/2006:15:04:05 -0700",
	}

	for _, format := range formats {
		if ts, err := time.Parse(format, timestamp); err == nil {
			return ts.Unix()
		}
	}

	// Try parsing as Unix timestamp string
	if i, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
		return i
	}

	return time.Now().Unix()
}
