package otlp

import (
	"sort"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// TypedAttrs partitions OTLP attributes into three typed maps
// (strings, numbers, bools) and caps the string map at maxStringKeys.
func TypedAttrs(kvs []*commonpb.KeyValue, maxStringKeys int) (
	strMap map[string]string,
	numMap map[string]float64,
	boolMap map[string]bool,
	dropped int,
) {
	strMap = make(map[string]string, len(kvs))
	numMap = make(map[string]float64)
	boolMap = make(map[string]bool)
	dropped = TypedAttrsInto(kvs, maxStringKeys, strMap, numMap, boolMap)
	return
}

func TypedAttrsInto(
	kvs []*commonpb.KeyValue,
	maxStringKeys int,
	strMap map[string]string,
	numMap map[string]float64,
	boolMap map[string]bool,
) int {
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
		return capStringMapDeterministic(strMap, maxStringKeys)
	}
	return 0
}

func capStringMapDeterministic(strMap map[string]string, max int) int {
	return CapStringMap(strMap, max)
}

func CapStringMap(strMap map[string]string, max int) int {
	if max <= 0 || len(strMap) <= max {
		return 0
	}
	keys := make([]string, 0, len(strMap))
	for k := range strMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	dropped := 0
	for _, k := range keys[max:] {
		delete(strMap, k)
		dropped++
	}
	return dropped
}
