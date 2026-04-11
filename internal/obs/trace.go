package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"time"
)

type traceKeyType struct{}

var traceKey = traceKeyType{}

func NewTraceID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func WithTrace(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceKey, traceID)
}

func TraceID(ctx context.Context) string {
	if v := ctx.Value(traceKey); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

type SpanType string

const (
	SpanTypeDB       SpanType = "db"
	SpanTypeAgent    SpanType = "agent"
	SpanTypeRuntime  SpanType = "runtime"
	SpanTypePlugin   SpanType = "plugin"
	SpanTypeInternal SpanType = "internal"
)

type Span struct {
	Name      string
	Type      SpanType
	StartTime time.Time
	Attrs     map[string]interface{}
	TraceID   string
	ParentID  string
}

var enableTracing = os.Getenv("ENABLE_TRACING") == "true"

func StartSpan(ctx context.Context, name string, spanType SpanType, attrs map[string]interface{}) (context.Context, *Span) {
	traceID := TraceID(ctx)
	if traceID == "" {
		traceID = NewTraceID()
		ctx = WithTrace(ctx, traceID)
	}

	span := &Span{
		Name:      name,
		Type:      spanType,
		StartTime: time.Now(),
		Attrs:     attrs,
		TraceID:   traceID,
	}

	if enableTracing {
		Info("span_start", Field{
			"trace_id": traceID,
			"name":     name,
			"type":     string(spanType),
		})
	}

	return ctx, span
}

func (s *Span) End(ctx context.Context, err error) {
	if !enableTracing {
		return
	}

	duration := time.Since(s.StartTime).Milliseconds()

	result := "success"
	if err != nil {
		result = "error"
		s.Attrs["error"] = err.Error()
	}

	Info("span_end", Field{
		"trace_id":    s.TraceID,
		"name":        s.Name,
		"type":        string(s.Type),
		"duration_ms": duration,
		"result":      result,
	})
}

func (s *Span) SetAttr(key string, value interface{}) {
	if s.Attrs == nil {
		s.Attrs = make(map[string]interface{})
	}
	s.Attrs[key] = value
}

func (s *Span) RecordError(err error) {
	if err != nil {
		s.SetAttr("error", err.Error())
	}
}

type TraceExporter interface {
	ExportSpan(ctx context.Context, span *Span) error
}

var exporters []TraceExporter

func RegisterExporter(e TraceExporter) {
	exporters = append(exporters, e)
}

func ExportSpan(ctx context.Context, span *Span) {
	for _, e := range exporters {
		if err := e.ExportSpan(ctx, span); err != nil {
			Error("trace_export_failed", Field{
				"error": err.Error(),
				"name":  span.Name,
			})
		}
	}
}

type FailureType string

const (
	FailureTypeInfra      FailureType = "infra_error"
	FailureTypeDependency FailureType = "dependency_error"
	FailureTypeUserCode   FailureType = "user_code_error"
	FailureTypeTimeout    FailureType = "timeout_error"
	FailureTypeMemory     FailureType = "memory_error"
	FailureTypeUnknown    FailureType = "unknown_error"
)

func ClassifyFailure(errMsg string) FailureType {
	errLower := errMsg

	switch {
	case contains(errLower, "connection refused"),
		contains(errLower, "timeout"),
		contains(errLower, "network"):
		return FailureTypeInfra
	case contains(errLower, "no module named"),
		contains(errLower, "cannot find module"),
		contains(errLower, "dependency"),
		contains(errLower, "install failed"):
		return FailureTypeDependency
	case contains(errLower, "syntax"),
		contains(errLower, "indentation"),
		contains(errLower, "nameerror"),
		contains(errLower, "typeerror"):
		return FailureTypeUserCode
	case contains(errLower, "memory"),
		contains(errLower, "oom"):
		return FailureTypeMemory
	default:
		return FailureTypeUnknown
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s[:len(substr)] == substr ||
			(len(s) > len(substr) && (containsAny(s, substr))))
}

func containsAny(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

type LogContext struct {
	TraceID  string
	JobID    string
	EnvKey   string
	SpanType SpanType
	OrgID    string
}

func WithLogContext(ctx context.Context, lc LogContext) context.Context {
	return context.WithValue(ctx, "log_context", lc)
}

func GetLogContext(ctx context.Context) LogContext {
	if v := ctx.Value("log_context"); v != nil {
		if lc, ok := v.(LogContext); ok {
			return lc
		}
	}
	return LogContext{}
}

func LogWithContext(level, msg string, ctx LogContext, fields Field) {
	if ctx.TraceID != "" {
		fields["trace_id"] = ctx.TraceID
	}
	if ctx.JobID != "" {
		fields["job_id"] = ctx.JobID
	}
	if ctx.EnvKey != "" {
		fields["env_key"] = ctx.EnvKey
	}
	if ctx.SpanType != "" {
		fields["span_type"] = string(ctx.SpanType)
	}
	if ctx.OrgID != "" {
		fields["org_id"] = ctx.OrgID
	}

	switch level {
	case "info":
		Info(msg, fields)
	case "warn":
		Warn(msg, fields)
	case "error":
		Error(msg, fields)
	default:
		Info(msg, fields)
	}
}

type TraceContextKey struct{}

func WithSpanContext(ctx context.Context, span *Span) context.Context {
	return context.WithValue(ctx, TraceContextKey{}, span)
}

func GetSpanContext(ctx context.Context) *Span {
	if v := ctx.Value(TraceContextKey{}); v != nil {
		if span, ok := v.(*Span); ok {
			return span
		}
	}
	return nil
}

func (lc LogContext) WithOrgID(orgID string) LogContext {
	lc.OrgID = orgID
	return lc
}

func (lc LogContext) WithJobID(jobID string) LogContext {
	lc.JobID = jobID
	return lc
}

func (lc LogContext) WithTraceID(traceID string) LogContext {
	lc.TraceID = traceID
	return lc
}

func (lc LogContext) WithEnvKey(envKey string) LogContext {
	lc.EnvKey = envKey
	return lc
}

func (lc LogContext) WithSpanType(spanType SpanType) LogContext {
	lc.SpanType = spanType
	return lc
}
