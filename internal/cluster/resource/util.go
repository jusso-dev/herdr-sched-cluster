package resource

import (
	"fmt"
	"strconv"
	"strings"
)

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// fieldsAfter returns the whitespace-separated fields of a line after a
// label, for the "key: value unit" shape /proc files use.
func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n
}

func atof(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return f
}
