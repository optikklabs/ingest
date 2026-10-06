package otlp

import (
	"maps"
	"slices"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func TypedAttrs(kvs []*commonpb.KeyValue, maxStringKeys int) (
	strMap map[string]string,
	numMap map[string]float64,
	boolMap map[string]bool,
	dropped int,
) {
	strMap = make(map[string]string, len(kvs))
	numMap = make(map[string]float64)
	boolMap = make(map[string]bool)
	for _, kv := range kvs {
		if kv == nil || kv.Value == nil {
			continue
		}
		switch val := kv.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			strMap[kv.Key] = val.StringValue
		case *commonpb.AnyValue_IntValue:
			numMap[kv.Key] = float64(val.IntValue)
		case *commonpb.AnyValue_DoubleValue:
			numMap[kv.Key] = val.DoubleValue
		case *commonpb.AnyValue_BoolValue:
			boolMap[kv.Key] = val.BoolValue
		case *commonpb.AnyValue_BytesValue:
			strMap[kv.Key] = string(val.BytesValue)
		}
	}
	if maxStringKeys > 0 && len(strMap) > maxStringKeys {
		dropped = CapStringMap(strMap, maxStringKeys)
	}
	return
}

// CapStringMap keeps the limit lexically smallest keys of strMap, deleting
// the rest, and returns how many it deleted.
func CapStringMap(strMap map[string]string, limit int) int {
	if limit <= 0 || len(strMap) <= limit {
		return 0
	}
	excess := slices.Sorted(maps.Keys(strMap))[limit:]
	for _, k := range excess {
		delete(strMap, k)
	}
	return len(excess)
}
