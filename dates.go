package filestore

import "time"

func ParseDBDate(date string) (time.Time, error) {
	layout := "2006-01-02 15:04:05"
	return time.Parse(layout, date)
}

func ToDBDate(date time.Time) string {
	return date.Format("2006-01-02 15:04:05")
}
