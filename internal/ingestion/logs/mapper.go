package logs

import (
	"fmt"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/infra/timebucket"
	"github.com/optikklabs/ingest/internal/ingestion/ingestionstats"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

const (
	nsPerSecond      = 1_000_000_000
	maxLogAttributes = 128
)

func mapRequest(tenantID int64, req *logspb.ExportLogsServiceRequest) ([]*schema.Row, []ingestionstats.ResourceUsage) {
	nowNs := uint64(time.Now().UnixNano())
	var rows []*schema.Row
	usage := make([]ingestionstats.ResourceUsage, 0, len(req.GetResourceLogs()))
	for _, rl := range req.GetResourceLogs() {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}

		rc := newResourceContext(otlp.AttrsToMap(resAttrs))
		before := len(rows)
		for _, sl := range rl.GetScopeLogs() {
			scopeName, scopeVersion := "", ""
			if sl.GetScope() != nil {
				scopeName = sl.GetScope().GetName()
				scopeVersion = sl.GetScope().GetVersion()
			}
			for _, lr := range sl.GetLogRecords() {
				rows = append(rows, buildLogRow(tenantID, rc, scopeName, scopeVersion, lr, nowNs))
			}
		}
		if n := len(rows) - before; n > 0 {
			usage = append(usage, ingestionstats.ResourceUsage{Service: rc.dims.Service, Environment: rc.dims.Environment, Records: uint64(n)})
		}
	}
	return rows, usage
}

var resourceFallbackKeys = []string{"service.name", "host.name", "k8s.pod.name", "deployment.environment"}

// resourceContext holds per-ResourceLogs state computed once instead of per
// log record. res is shared read-only by rows that need no fallback patch;
// rows are marshaled synchronously in this request and never mutated after.
type resourceContext struct {
	res     map[string]string
	dims    fingerprint.ResourceDimensions
	missing []string
}

func newResourceContext(resourceMap map[string]string) resourceContext {
	res := make(map[string]string, len(resourceMap))
	for k, v := range resourceMap {
		res[k] = v
	}
	var missing []string
	for _, k := range resourceFallbackKeys {
		if res[k] == "" {
			missing = append(missing, k)
		}
	}
	return resourceContext{res: res, dims: fingerprint.ResolveResource(res), missing: missing}
}

// resolveResource patches fallback keys from record attrs only when the
// resource is genuinely missing them, copying the shared map at most once.
func (rc resourceContext) resolveResource(attrs map[string]string) (map[string]string, fingerprint.ResourceDimensions) {
	res := rc.res
	patched := false
	for _, k := range rc.missing {
		v := attrs[k]
		if v == "" {
			continue
		}
		if !patched {
			m := make(map[string]string, len(rc.res)+len(rc.missing))
			for k2, v2 := range rc.res {
				m[k2] = v2
			}
			res = m
			patched = true
		}
		res[k] = v
	}
	if !patched {
		return rc.res, rc.dims
	}
	return res, fingerprint.ResolveResource(res)
}

func buildLogRow(tenantID int64, rc resourceContext, scopeName, scopeVersion string, lr *logv1.LogRecord, nowNs uint64) *schema.Row {
	tsNs := lr.GetTimeUnixNano()
	if tsNs == 0 {
		tsNs = lr.GetObservedTimeUnixNano()
	}
	if tsNs == 0 {
		tsNs = nowNs
	}
	observedNs := lr.GetObservedTimeUnixNano()
	if observedNs == 0 {
		observedNs = nowNs
	}
	tsBucket := timebucket.BucketStart(int64(tsNs / nsPerSecond))

	attrStr, attrNum, attrBool, dropped := otlp.TypedAttrs(lr.GetAttributes(), maxLogAttributes)
	if dropped > 0 {
		obsmetrics.MapperAttrsDropped.WithLabelValues("logs").Add(float64(dropped))
	}
	sevNum := uint32(lr.GetSeverityNumber())
	res, dims := rc.resolveResource(attrStr)

	traceID := otlp.BytesToHex(lr.GetTraceId())
	body := otlp.AnyValueString(lr.GetBody())
	logID := computeLogID(traceID, tsNs, body)

	return &schema.Row{
		TenantId:            uint32(tenantID),
		TsBucket:            tsBucket,
		TimestampNs:         int64(tsNs),
		ObservedTimestampNs: observedNs,
		TraceId:             traceID,
		SpanId:              otlp.BytesToHex(lr.GetSpanId()),
		TraceFlags:          lr.GetFlags(),
		SeverityText:        resolveSeverity(lr),
		SeverityNumber:      sevNum,
		Body:                body,
		AttributesString:    attrStr,
		AttributesNumber:    attrNum,
		AttributesBool:      attrBool,
		Resource:            res,
		LogId:               logID,
		ScopeName:           scopeName,
		ScopeVersion:        scopeVersion,
		Service:             dims.Service,
		Host:                dims.Host,
		Pod:                 dims.Pod,
		Container:           dims.Container,
		Environment:         dims.Environment,
	}
}

// computeLogID is a stable FNV-1a hash of (trace_id, timestamp, body).
// IDs are stored, so the hash inputs and hex format must never change.
func computeLogID(traceID string, tsNs uint64, body string) string {
	h := fnv.New64a()
	_, _ = io.WriteString(h, traceID)
	_, _ = h.Write([]byte{255})
	_, _ = io.WriteString(h, strconv.FormatUint(tsNs, 10))
	_, _ = h.Write([]byte{255})
	_, _ = io.WriteString(h, body)
	return fmt.Sprintf("%016x", h.Sum64())
}

// resolveSeverity prefers the record's text, falling back to the numeric
// level; the result is always trimmed and upper-cased.
func resolveSeverity(lr *logv1.LogRecord) string {
	if s := strings.ToUpper(strings.TrimSpace(lr.GetSeverityText())); s != "" {
		return s
	}
	n := int(lr.GetSeverityNumber())
	switch {
	case n <= 0:
		return "UNSET"
	case n <= 4:
		return "TRACE"
	case n <= 8:
		return "DEBUG"
	case n <= 12:
		return "INFO"
	case n <= 16:
		return "WARN"
	case n <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}

