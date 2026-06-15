package spans

import (
	"encoding/json"
	"strconv"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/infra/timebucket"
	"github.com/optikklabs/ingest/internal/ingestion/spans/schema"
	tracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	trace "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	nsPerSecond       = 1_000_000_000
	maxSpanAttributes = 128
	zeroTraceHex      = "00000000000000000000000000000000"
	zeroSpanHex       = "0000000000000000"
)

// mapRequest converts an OTLP trace export request into wire rows; one OTLP
// span yields one Row.
func mapRequest(teamID int64, req *tracepb.ExportTraceServiceRequest) []*schema.Row {
	rows := make([]*schema.Row, 0, 64)
	for _, rs := range req.GetResourceSpans() {
		var resAttrs []*commonpb.KeyValue
		if rs.Resource != nil {
			resAttrs = rs.Resource.Attributes
		}
		resMap := otlp.AttrsToMap(resAttrs)
		fp := fingerprint.CalculateHash(resMap)
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				rows = append(rows, buildSpanRow(teamID, resMap, fp, s))
			}
		}
	}
	return rows
}

func buildSpanRow(teamID int64, resMap map[string]string, fp uint64, s *trace.Span) *schema.Row {
	timestampNs := s.GetStartTimeUnixNano()
	tsBucket := timebucket.BucketStart(int64(timestampNs / nsPerSecond))

	statusMsg := ""
	statusCode := trace.Status_STATUS_CODE_UNSET
	if s.Status != nil {
		statusMsg = s.Status.GetMessage()
		statusCode = s.Status.GetCode()
	}

	spanMap := otlp.AttrsToMap(s.GetAttributes())
	merged := mergeAndCapAttrs(resMap, spanMap)

	httpMethod := firstNonEmpty(spanMap, "http.method", "http.request.method")
	httpURL := firstNonEmpty(spanMap, "http.url", "url.full")
	httpHost := firstNonEmpty(spanMap, "http.host", "net.host.name")
	httpStatus := firstNonEmpty(spanMap, "http.status_code", "http.response.status_code")

	stripPromotedKeys(merged)

	return &schema.Row{
		TsBucket:            uint64(tsBucket),
		TeamId:              uint32(teamID),
		TimestampNs:         int64(timestampNs),
		TraceId:             zeroOut(otlp.BytesToHex(s.GetTraceId()), zeroTraceHex),
		SpanId:              zeroOut(otlp.BytesToHex(s.GetSpanId()), zeroSpanHex),
		ParentSpanId:        zeroOut(otlp.BytesToHex(s.GetParentSpanId()), zeroSpanHex),
		TraceState:          s.GetTraceState(),
		Flags:               s.GetFlags(),
		Name:                s.GetName(),
		Kind:                int32(s.GetKind()),
		KindString:          spanKindString(s.GetKind()),
		DurationNano:        spanDuration(s),
		HasError:            statusCode == trace.Status_STATUS_CODE_ERROR,
		StatusCode:          int32(statusCode),
		StatusCodeString:    statusCodeString(statusCode),
		StatusMessage:       statusMsg,
		HttpUrl:             httpURL,
		HttpMethod:          httpMethod,
		HttpHost:            httpHost,
		ResponseStatusCode:  httpStatus,
		HttpStatusBucket:    httpStatusBucket(httpStatus, statusCode == trace.Status_STATUS_CODE_ERROR),
		Service:             resMap["service.name"],
		Host:                resMap["host.name"],
		Pod:                 resMap["k8s.pod.name"],
		ServiceVersion:      resMap["service.version"],
		Environment:         resMap["deployment.environment"],
		PeerService:         spanMap["peer.service"],
		DbSystem:            spanMap["db.system"],
		DbName:              spanMap["db.name"],
		DbStatement:         spanMap["db.statement"],
		HttpRoute:           spanMap["http.route"],
		Attributes:          merged,
		Fingerprint:         fp,
		Events:              serializeEvents(s.GetEvents()),
		Links:               serializeLinks(s.GetLinks()),
		ExceptionType:       spanMap["exception.type"],
		ExceptionMessage:    spanMap["exception.message"],
		ExceptionStacktrace: spanMap["exception.stacktrace"],
		ExceptionEscaped:    spanMap["exception.escaped"] == "true",
	}
}

func mergeAndCapAttrs(resMap, spanMap map[string]string) map[string]string {
	merged := make(map[string]string, len(resMap)+len(spanMap))
	for k, v := range resMap {
		merged[k] = v
	}
	for k, v := range spanMap {
		merged[k] = v
	}
	if dropped := otlp.CapStringMap(merged, maxSpanAttributes); dropped > 0 {
		obsmetrics.MapperAttrsDropped.WithLabelValues("spans").Add(float64(dropped))
	}
	return merged
}

func spanDuration(s *trace.Span) uint64 {
	if s.EndTimeUnixNano > s.StartTimeUnixNano {
		return s.EndTimeUnixNano - s.StartTimeUnixNano
	}
	return 0
}

func serializeEvents(events []*trace.Span_Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		ev := map[string]any{"name": e.Name}
		if e.TimeUnixNano > 0 {
			ev["timeUnixNano"] = strconv.FormatUint(e.TimeUnixNano, 10)
		}
		if len(e.Attributes) > 0 {
			ev["attributes"] = otlp.AttrsToMap(e.Attributes)
		}
		b, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		out = append(out, string(b))
	}
	return out
}

func serializeLinks(links []*trace.Span_Link) string {
	data := make([]map[string]any, 0, len(links))
	for _, lk := range links {
		link := map[string]any{
			"traceId": otlp.BytesToHex(lk.TraceId),
			"spanId":  otlp.BytesToHex(lk.SpanId),
		}
		if len(lk.Attributes) > 0 {
			link["attributes"] = otlp.AttrsToMap(lk.Attributes)
		}
		data = append(data, link)
	}
	b, _ := json.Marshal(data)
	return string(b)
}

func spanKindString(k trace.Span_SpanKind) string {
	switch k {
	case trace.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	case trace.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case trace.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case trace.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case trace.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
}

func statusCodeString(c trace.Status_StatusCode) string {
	switch c {
	case trace.Status_STATUS_CODE_OK:
		return "OK"
	case trace.Status_STATUS_CODE_ERROR:
		return "ERROR"
	default:
		return "UNSET"
	}
}

func httpStatusBucket(statusCode string, hasError bool) string {
	if statusCode != "" {
		if n, err := strconv.ParseUint(statusCode, 10, 16); err == nil {
			switch {
			case n >= 500:
				return "5xx"
			case n >= 400:
				return "4xx"
			case n >= 300:
				return "3xx"
			case n >= 200:
				return "2xx"
			}
		}
	}
	if hasError {
		return "err"
	}
	return "other"
}

func zeroOut(id, zero string) string {
	if id == zero {
		return ""
	}
	return id
}

func firstNonEmpty(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := m[k]; v != "" {
			return v
		}
	}
	return ""
}

// promotedSpanKeys are OTLP attribute keys promoted to dedicated CH columns.
// These are stripped post-extraction to prevent duplicate JSON data.
var promotedSpanKeys = []string{
	"http.method", "http.request.method",
	"http.url", "url.full",
	"http.host", "net.host.name",
	"http.status_code", "http.response.status_code",
	"exception.type", "exception.message", "exception.stacktrace", "exception.escaped",
	"service.name", "host.name", "k8s.pod.name", "service.version", "deployment.environment",
	"peer.service", "db.system", "db.name", "db.statement", "http.route",
}

func stripPromotedKeys(m map[string]string) {
	for _, k := range promotedSpanKeys {
		delete(m, k)
	}
}
