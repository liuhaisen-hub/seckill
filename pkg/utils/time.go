package utils

import "time"

func ParserTimeString(t string) (time.Time, error) {
	return time.Parse(time.RFC3339, t)
}

func FormatTime(t time.Time) string {
	return t.Format("RFC3339")
}
