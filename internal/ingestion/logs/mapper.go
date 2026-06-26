package logs

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/optikklabs/ingest/internal/infra/fingerprint"
	obsmetrics "github.com/optikklabs/ingest/internal/infra/metrics"
	"github.com/optikklabs/ingest/internal/infra/otlp"
	"github.com/optikklabs/ingest/internal/infra/timebucket"
	"github.com/optikklabs/ingest/internal/ingestion/logs/schema"
	logspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

const (
	nsPerSecond      = 1_000_000_000
	maxLogAttributes = 128
	zeroTraceHex     = "00000000000000000000000000000000"
	zeroSpanHex      = "0000000000000000"
)

func mapRequest(teamID int64, req *logspb.ExportLogsServiceRequest) []*schema.Row {
	nowNs := uint64(time.Now().UnixNano())
	var rows []*schema.Row
	for _, rl := range req.GetResourceLogs() {
		var resAttrs []*commonpb.KeyValue
		if rl.Resource != nil {
			resAttrs = rl.Resource.Attributes
		}
		resourceMap := otlp.AttrsToMap(resAttrs)
		fp := fingerprint.CalculateHash(resourceMap)
		for _, sl := range rl.GetScopeLogs() {
			scopeName, scopeVersion := "", ""
			if sl.GetScope() != nil {
				scopeName = sl.GetScope().GetName()
				scopeVersion = sl.GetScope().GetVersion()
			}
			for _, lr := range sl.GetLogRecords() {
				rows = append(rows, buildLogRow(teamID, resourceMap, fp, scopeName, scopeVersion, lr, nowNs))
			}
		}
	}
	return rows
}

func buildLogRow(teamID int64, resource map[string]string, fp uint64, scopeName, scopeVersion string, lr *logv1.LogRecord, nowNs uint64) *schema.Row {
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
	res := fillResourceFallbacks(resource, attrStr)

	traceID := zeroOut(otlp.BytesToHex(lr.GetTraceId()), zeroTraceHex)
	body := otlp.AnyValueString(lr.GetBody())
	logID := computeLogID(traceID, tsNs, body, fp)

	return &schema.Row{
		TeamId:              uint32(teamID),
		TsBucket:            tsBucket,
		TimestampNs:         int64(tsNs),
		ObservedTimestampNs: observedNs,
		TraceId:             traceID,
		SpanId:              zeroOut(otlp.BytesToHex(lr.GetSpanId()), zeroSpanHex),
		TraceFlags:          lr.GetFlags(),
		SeverityText:        normalizeSeverityText(resolveSeverity(lr), sevNum),
		SeverityNumber:      sevNum,
		Body:                body,
		AttributesString:    attrStr,
		AttributesNumber:    attrNum,
		AttributesBool:      attrBool,
		Resource:            res,
		Fingerprint:         fp,
		LogId:               logID,
		ScopeName:           scopeName,
		ScopeVersion:        scopeVersion,
		Service:             res["service.name"],
		Host:                res["host.name"],
		Pod:                 res["k8s.pod.name"],
		Container:           res["k8s.container.name"],
		Environment:         res["deployment.environment"],
	}
}

// computeLogID returns a stable FNV-64a hex hash of the log attributes.
// This is used as the row's deep-link ID.
func computeLogID(traceID string, tsNs uint64, body string, fp uint64) string {
	const (
		offset64      uint64 = 14695981039346656037
		prime64       uint64 = 1099511628211
		separatorByte byte   = 255
	)
	addStr := func(h uint64, s string) uint64 {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= prime64
		}
		return h
	}
	addByte := func(h uint64, b byte) uint64 {
		h ^= uint64(b)
		h *= prime64
		return h
	}
	h := offset64
	h = addStr(h, traceID)
	h = addByte(h, separatorByte)
	h = addStr(h, strconv.FormatUint(tsNs, 10))
	h = addByte(h, separatorByte)
	h = addStr(h, body)
	h = addByte(h, separatorByte)
	h = addStr(h, strconv.FormatUint(fp, 10))
	return fmt.Sprintf("%016x", h)
}

func resolveSeverity(lr *logv1.LogRecord) string {
	if s := lr.GetSeverityText(); s != "" {
		return s
	}
	return severityNumberToLevel(lr.GetSeverityNumber())
}

func severityBucketFor(severityNumber uint32) uint8 {
	switch {
	case severityNumber >= 21:
		return 5
	case severityNumber >= 17:
		return 4
	case severityNumber >= 13:
		return 3
	case severityNumber >= 9:
		return 2
	case severityNumber >= 5:
		return 1
	default:
		return 0
	}
}

func severityNumberToLevel(n logv1.SeverityNumber) string {
	v := int(n)
	switch {
	case v <= 0:
		return "UNSET"
	case v <= 4:
		return "TRACE"
	case v <= 8:
		return "DEBUG"
	case v <= 12:
		return "INFO"
	case v <= 16:
		return "WARN"
	case v <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}

func normalizeSeverityText(text string, num uint32) string {
	t := strings.ToUpper(strings.TrimSpace(text))
	if t != "" {
		return t
	}
	switch {
	case num == 0:
		return "UNSET"
	case num <= 4:
		return "TRACE"
	case num <= 8:
		return "DEBUG"
	case num <= 12:
		return "INFO"
	case num <= 16:
		return "WARN"
	case num <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}

func fillResourceFallbacks(resource, attrs map[string]string) map[string]string {
	if resource == nil {
		resource = map[string]string{}
	}
	for _, k := range []string{"service.name", "host.name", "k8s.pod.name", "deployment.environment"} {
		if resource[k] != "" {
			continue
		}
		if v := attrs[k]; v != "" {
			resource[k] = v
		}
	}
	return resource
}

func zeroOut(id, zero string) string {
	if id == zero {
		return ""
	}
	return id
}
