package database

// resultLabel maps a DB error into the {ok,err} label used by the
// optikk_db_queries_total counter. Any non-nil error → "err".
func resultLabel(err error) string {
	if err != nil {
		return "err"
	}
	return "ok"
}
