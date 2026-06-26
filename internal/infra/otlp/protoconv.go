package otlp

import (
	"encoding/hex"
	"fmt"
	"strconv"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

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
		// For maps and arrays, we use the pdata-like string representation
		return fmt.Sprintf("%v", v.Value)
	}
}

func AttrsToMap(kvs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = AnyValueString(kv.Value)
	}
	return m
}

func BytesToHex(b []byte) string {
	if len(b) > 0 {
		return hex.EncodeToString(b)
	}
	return ""
}
