package spans

import (
	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/infra/timebucket"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
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

func mapRequest(tenantID int64, req *tracepb.ExportTraceServiceRequest) ([]*schema.Row, []ingestionstats.ResourceUsage) {
	spanCount := 0
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			spanCount += len(ss.GetSpans())
		}
	}
	if spanCount == 0 {
		return nil, nil
	}
	rows := make([]*schema.Row, 0, spanCount)
	usage := make([]ingestionstats.ResourceUsage, 0, len(req.GetResourceSpans()))
	for _, rs := range req.GetResourceSpans() {
		var resAttrs []*commonpb.KeyValue
		if rs.Resource != nil {
			resAttrs = rs.Resource.Attributes
		}

		resMap := otlp.GetAttrMap()
		otlp.AttrsToMapInto(resMap, resAttrs)
		dims := fingerprint.ResolveResource(resMap)
		baseAttrs := resourceBaseAttrs(resMap)
		before := len(rows)
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				rows = append(rows, buildSpanRow(tenantID, baseAttrs, dims, s))
			}
		}
		otlp.PutAttrMap(resMap)
		if n := len(rows) - before; n > 0 {
			usage = append(usage, ingestionstats.ResourceUsage{Service: dims.Service, Environment: dims.Environment, Records: uint64(n)})
		}
	}
	return rows, usage
}

// resourceBaseAttrs filters promoted keys and caps once per ResourceSpans,
// instead of re-copying all resource attrs for every span in the batch.
func resourceBaseAttrs(resMap map[string]string) map[string]string {
	base := make(map[string]string, len(resMap))
	for k, v := range resMap {
		if !isPromotedKey(k) {
			base[k] = v
		}
	}
	if dropped := otlp.CapStringMap(base, maxSpanAttributes); dropped > 0 {
		obsmetrics.MapperAttrsDropped.WithLabelValues("spans").Add(float64(dropped))
	}
	return base
}

func buildSpanRow(tenantID int64, baseAttrs map[string]string, dims fingerprint.ResourceDimensions, s *trace.Span) *schema.Row {
	timestampNs := s.GetStartTimeUnixNano()
	tsBucket := timebucket.BucketStart(int64(timestampNs / nsPerSecond))

	statusMsg := ""
	statusCode := trace.Status_STATUS_CODE_UNSET
	if s.Status != nil {
		statusMsg = s.Status.GetMessage()
		statusCode = s.Status.GetCode()
	}

	spanMap := otlp.GetAttrMap()
	defer otlp.PutAttrMap(spanMap)
	otlp.AttrsToMapInto(spanMap, s.GetAttributes())
	merged := mergeAndCapAttrs(baseAttrs, spanMap)

	httpMethod := firstNonEmpty(spanMap, "http.method", "http.request.method")
	httpURL := firstNonEmpty(spanMap, "http.url", "url.full")
	httpHost := firstNonEmpty(spanMap, "http.host", "net.host.name")
	httpStatus := firstNonEmpty(spanMap, "http.status_code", "http.response.status_code")
	gen := extractGenAI(spanMap, spanDuration(s))

	return &schema.Row{
		TsBucket:            uint64(tsBucket),
		TenantId:            uint32(tenantID),
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
		Service:             dims.Service,
		Host:                dims.Host,
		Pod:                 dims.Pod,
		ServiceVersion:      dims.Version,
		Environment:         dims.Environment,
		PeerService:         spanMap["peer.service"],
		DbSystem:            spanMap["db.system"],
		DbName:              spanMap["db.name"],
		DbStatement:         spanMap["db.statement"],
		HttpRoute:           spanMap["http.route"],
		Attributes:          merged,
		Events:              serializeEvents(s.GetEvents()),
		Links:               serializeLinks(s.GetLinks()),
		ExceptionType:       spanMap["exception.type"],
		ExceptionMessage:    spanMap["exception.message"],
		ExceptionStacktrace: spanMap["exception.stacktrace"],
		ExceptionEscaped:    spanMap["exception.escaped"] == "true",
		GenAiSystem:         gen.System,
		GenAiOperation:      gen.Operation,
		GenAiRequestModel:   gen.RequestModel,
		GenAiResponseModel:  gen.ResponseModel,
		GenAiPrompt:         gen.Prompt,
		GenAiCompletion:     gen.Completion,
		GenAiInputTokens:    gen.InputTokens,
		GenAiOutputTokens:   gen.OutputTokens,
		IsGenAi:             gen.Present,
		LlmUserId:           gen.UserID,
		LlmSessionId:        gen.SessionID,
		LlmTags:             gen.Tags,
		LlmRelease:          gen.Release,
		LlmPromptName:       gen.PromptName,
		LlmPromptVersion:    gen.PromptVersion,
		GenAiSpanKind:       gen.SpanKind,
	}
}

// mergeAndCapAttrs merges span attrs over the precomputed resource base.
// Spans with no own attrs alias baseAttrs: rows are marshaled synchronously
// in this request and never mutated afterwards, so sharing one read-only
// map across spans is safe and skips a full copy per span.
func mergeAndCapAttrs(baseAttrs, spanMap map[string]string) map[string]string {
	hasOwn := false
	for k := range spanMap {
		if !isPromotedKey(k) {
			hasOwn = true
			break
		}
	}
	if !hasOwn {
		return baseAttrs
	}
	merged := make(map[string]string, len(baseAttrs)+len(spanMap))
	for k, v := range baseAttrs {
		merged[k] = v
	}
	for k, v := range spanMap {
		if !isPromotedKey(k) {
			merged[k] = v
		}
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

func serializeEvents(events []*trace.Span_Event) []*schema.Row_SpanEvent {
	if len(events) == 0 {
		return nil
	}
	out := make([]*schema.Row_SpanEvent, 0, len(events))
	for _, e := range events {
		out = append(out, &schema.Row_SpanEvent{
			Name:         e.Name,
			TimeUnixNano: e.TimeUnixNano,
			Attributes:   otlp.AttrsToMap(e.Attributes),
		})
	}
	return out
}

func serializeLinks(links []*trace.Span_Link) []*schema.Row_SpanLink {
	if len(links) == 0 {
		return nil
	}
	out := make([]*schema.Row_SpanLink, 0, len(links))
	for _, lk := range links {
		out = append(out, &schema.Row_SpanLink{
			TraceId:    otlp.BytesToHex(lk.TraceId),
			SpanId:     otlp.BytesToHex(lk.SpanId),
			TraceState: lk.TraceState,
			Attributes: otlp.AttrsToMap(lk.Attributes),
		})
	}
	return out
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

var promotedSpanKeys = []string{
	"http.method", "http.request.method",
	"http.url", "url.full",
	"http.host", "net.host.name",
	"http.status_code", "http.response.status_code",
	"exception.type", "exception.message", "exception.stacktrace", "exception.escaped",
	"service.name", "host.name", "k8s.pod.name", "service.version", "deployment.environment",
	"peer.service", "db.system", "db.name", "db.statement", "http.route",
	"gen_ai.system", "gen_ai.operation.name", "gen_ai.request.model", "gen_ai.response.model",
	"gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens",
	"gen_ai.usage.prompt_tokens", "gen_ai.usage.completion_tokens",
	"gen_ai.prompt", "gen_ai.completion",
	"gen_ai.input.messages", "gen_ai.output.messages",
	"gen_ai.request.user", "user.id", "enduser.id", "langfuse.user.id",
	"gen_ai.conversation.id", "session.id", "langfuse.session.id",
	"langfuse.release", "langfuse.trace.tags", "optikk.llm.tags",
	"langfuse.prompt.name", "optikk.prompt.name",
	"langfuse.prompt.version", "optikk.prompt.version",
	"langfuse.observation.type", "gen_ai.observation.type", "optikk.eval",
}

var promotedKeysMap = buildPromotedKeysMap()

func buildPromotedKeysMap() map[string]bool {
	m := make(map[string]bool, len(promotedSpanKeys))
	for _, k := range promotedSpanKeys {
		m[k] = true
	}
	return m
}

func isPromotedKey(k string) bool {
	return promotedKeysMap[k]
}
