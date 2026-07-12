package otlp

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

func strVal(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

func intVal(i int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: i}}
}

// TestAnyValueStringScalars locks the scalar formatting.
func TestAnyValueStringScalars(t *testing.T) {
	tests := []struct {
		name string
		in   *commonpb.AnyValue
		want string
	}{
		{"nil", nil, ""},
		{"string", strVal("hello"), "hello"},
		{"int", intVal(42), "42"},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 1.5}}, "1.5"},
		{"bool", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"bytes", &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0xab, 0xcd}}}, "abcd"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AnyValueString(tt.in); got != tt.want {
				t.Errorf("AnyValueString() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAnyValueStringKvlistDeterministic proves a kvlist renders with sorted
// keys regardless of input order — the fingerprint-stability win.
func TestAnyValueStringKvlistDeterministic(t *testing.T) {
	kvlist := func(pairs ...*commonpb.KeyValue) *commonpb.AnyValue {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{
			KvlistValue: &commonpb.KeyValueList{Values: pairs},
		}}
	}
	kv := func(k string, v *commonpb.AnyValue) *commonpb.KeyValue {
		return &commonpb.KeyValue{Key: k, Value: v}
	}

	a := kvlist(kv("b", intVal(2)), kv("a", strVal("x")))
	b := kvlist(kv("a", strVal("x")), kv("b", intVal(2)))

	got, want := AnyValueString(a), `{"a":"x","b":2}`
	if got != want {
		t.Errorf("kvlist = %q, want %q", got, want)
	}
	if AnyValueString(a) != AnyValueString(b) {
		t.Errorf("kvlist ordering not deterministic: %q vs %q", AnyValueString(a), AnyValueString(b))
	}
}

// TestAnyValueStringArrayOrdered keeps array order (arrays are ordered).
func TestAnyValueStringArrayOrdered(t *testing.T) {
	arr := &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{
		ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{strVal("z"), strVal("a")}},
	}}
	if got, want := AnyValueString(arr), `["z","a"]`; got != want {
		t.Errorf("array = %q, want %q", got, want)
	}
}
