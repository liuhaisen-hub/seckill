package data

import "time"

// timeLayout 是 DO（string）与 PO（time.Time）之间统一使用的时间格式。
const timeLayout = time.RFC3339

func parseTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(timeLayout, value)
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(timeLayout)
}
