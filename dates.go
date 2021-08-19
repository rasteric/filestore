package filestore

import "time"

func parseDBDate(date string) (time.Time, error) {
	layout := "2006-01-02T15:04:05-0700"
	return time.Parse(layout, date)
}

func toDBDate(date time.Time) string {
	return date.Format("2006-01-02 15:04:05")
}
