package database

func resultLabel(err error) string {
	if err != nil {
		return "err"
	}
	return "ok"
}
