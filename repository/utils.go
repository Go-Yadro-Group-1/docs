package repository

import (
	"database/sql"
	"time"
)

func NullStringFrom(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func NullInt64From(i int64) sql.NullInt64 {
	return sql.NullInt64{Int64: i, Valid: true}
}

func NullTimeFrom(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}
