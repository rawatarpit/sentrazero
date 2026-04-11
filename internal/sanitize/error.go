package sanitize

import (
	"encoding/json"
	"regexp"
)

var (
	pathPattern        = regexp.MustCompile(`/[^/\s]+/[^/\s]+`)
	windowsPathPattern = regexp.MustCompile(`[A-Za-z]:\\[^\\]+\\[^\\]+`)
	s3PathPattern      = regexp.MustCompile(`s3://[^\s]+`)
	gsPathPattern      = regexp.MustCompile(`gs://[^\s]+`)
	httpPathPattern    = regexp.MustCompile(`https://[^/]+/[^?\s]+`)
	homePathPattern    = regexp.MustCompile(`~[^/\s]+`)
	ipPathPattern      = regexp.MustCompile(`[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[^\s]+`)
)

func ErrorMessage(msg string) string {
	result := msg

	result = pathPattern.ReplaceAllString(result, "[PATH]")
	result = windowsPathPattern.ReplaceAllString(result, "[PATH]")

	result = s3PathPattern.ReplaceAllString(result, "s3://[BUCKET]")
	result = gsPathPattern.ReplaceAllString(result, "gs://[BUCKET]")
	result = httpPathPattern.ReplaceAllString(result, "https://[STORAGE]/[OBJECT]")

	result = homePathPattern.ReplaceAllString(result, "~[USER]")
	result = ipPathPattern.ReplaceAllString(result, "[IP]/[PATH]")

	result = sanitizeEmailAddresses(result)

	return result
}

func sanitizeEmailAddresses(msg string) string {
	emailPattern := regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)
	return emailPattern.ReplaceAllString(msg, "[EMAIL]")
}

func PayloadContainsPaths(payload map[string]interface{}) bool {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	return pathPattern.Match(encoded) ||
		windowsPathPattern.Match(encoded) ||
		s3PathPattern.Match(encoded) ||
		gsPathPattern.Match(encoded) ||
		httpPathPattern.Match(encoded) ||
		homePathPattern.Match(encoded) ||
		ipPathPattern.Match(encoded)
}

func SanitizePayload(payload map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range payload {
		switch val := v.(type) {
		case string:
			result[k] = ErrorMessage(val)
		case map[string]interface{}:
			result[k] = SanitizePayload(val)
		case []interface{}:
			result[k] = sanitizeSlice(val)
		default:
			result[k] = val
		}
	}
	return result
}

func sanitizeSlice(arr []interface{}) []interface{} {
	result := make([]interface{}, len(arr))
	for i, v := range arr {
		switch val := v.(type) {
		case string:
			result[i] = ErrorMessage(val)
		case map[string]interface{}:
			result[i] = SanitizePayload(val)
		case []interface{}:
			result[i] = sanitizeSlice(val)
		default:
			result[i] = val
		}
	}
	return result
}

func LogSafe(logger func(string, ...interface{}), msg string, args ...interface{}) {
	sanitized := ErrorMessage(msg)
	logger(sanitized, args...)
}

func AppendSanitizedError(errors []string, err error) []string {
	if err == nil {
		return errors
	}
	return append(errors, ErrorMessage(err.Error()))
}
