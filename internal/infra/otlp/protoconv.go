package otlp

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// attrMapPool reuses transient attribute maps in the hot mapping path. Only
// pool maps that are fully consumed within a single mapRequest — never maps
// retained on a Row (e.g. the merged Attributes).
var attrMapPool = sync.Pool{
	New: func() any { return make(map[string]string, 16) },
}

// GetAttrMap borrows a cleared attribute map from the pool.
func GetAttrMap() map[string]string {
	return attrMapPool.Get().(map[string]string)
}

// PutAttrMap clears and returns a map to the pool. Never pass a map that is
// still referenced by a Row.
func PutAttrMap(m map[string]string) {
	clear(m)
	attrMapPool.Put(m)
}

func AnyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	return pcommonValueToString(v)
}

func pcommonValueToString(v *commonpb.AnyValue) string {
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(val.BoolValue)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	default:
		// Maps/arrays: JSON with sorted keys so equal attribute sets hash to
		// one fingerprint (fmt %v map ordering was non-deterministic).
		b, err := json.Marshal(anyValueToGo(v))
		if err != nil {
			return ""
		}
		return string(b)
	}
}

// anyValueToGo converts an OTLP AnyValue into plain Go values so json.Marshal
// can render maps/arrays deterministically (map keys sorted).
func anyValueToGo(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return val.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonpb.AnyValue_BoolValue:
		return val.BoolValue
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonpb.AnyValue_KvlistValue:
		m := make(map[string]any, len(val.KvlistValue.GetValues()))
		for _, kv := range val.KvlistValue.GetValues() {
			m[kv.GetKey()] = anyValueToGo(kv.GetValue())
		}
		return m
	case *commonpb.AnyValue_ArrayValue:
		arr := make([]any, len(val.ArrayValue.GetValues()))
		for i, e := range val.ArrayValue.GetValues() {
			arr[i] = anyValueToGo(e)
		}
		return arr
	default:
		return nil
	}
}

func AttrsToMap(kvs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	AttrsToMapInto(m, kvs)
	return m
}

// AttrsToMapInto fills dst from kvs. The caller owns dst (e.g. a pooled map)
// and must clear it before reuse.
func AttrsToMapInto(dst map[string]string, kvs []*commonpb.KeyValue) {
	for _, kv := range kvs {
		dst[kv.Key] = AnyValueString(kv.Value)
	}
}

func BytesToHex(b []byte) string {
	if len(b) > 0 {
		return hex.EncodeToString(b)
	}
	return ""
}
