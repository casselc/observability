package hdxadapter

import (
	"regexp"
	"strconv"
)

func strconvI(n int) string { return strconv.Itoa(n) }

// clientErrorRE is @clickhouse/client-common 1.23's errorRe (dist/error/error.js).
var clientErrorRE = regexp.MustCompile(`(?s)(Code|Error): (\d+).*Exception: (?:.+?)\(((?:[A-Z0-9_]*[A-Z]{3}[A-Z0-9_]*))\)`)
