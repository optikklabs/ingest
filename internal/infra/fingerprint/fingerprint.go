package fingerprint

// CalculateHash produces a stable uint64 hash of identity attributes.
func CalculateHash(attrs map[string]string) uint64 {
	hierarchy := ResourceHierarchy()
	id := hierarchy.Identifier(attrs)
	idMap := make(map[string]string, len(id))
	for _, lv := range id {
		idMap[lv.Label] = lv.Value
	}
	return FingerprintHash(idMap)
}

// highCardinalityKeys are attribute keys excluded from series identity to cap
// cardinality, mirroring collector-side label drops. Excluded from the
// fingerprint hash only — values are still stored for display. Dropping a key
// collapses series that differ only by it, so their rollup values merge; tune
// this list to your telemetry before adding keys that carry aggregation meaning.
var highCardinalityKeys = map[string]struct{}{
	"k8s.pod.uid":        {},
	"k8s.replicaset.uid": {},
	"container.id":       {},
	"process.pid":        {},
}

// SeriesHash produces a full time-series identity for metrics, where
// resource attributes and data-point attributes are flattened together into
// a single map, excluding high-cardinality keys. The temporality and metric name
// are added as "__temporality__" and "__name__", and the combined map is
// sorted and hashed using FingerprintHash (xxhash).
func SeriesHash(metricName, temporality string, resAttrs, dpAttrs map[string]string) uint64 {
	merged := make(map[string]string, len(resAttrs)+len(dpAttrs)+2)
	for k, v := range resAttrs {
		if _, drop := highCardinalityKeys[k]; !drop {
			merged[k] = v
		}
	}
	for k, v := range dpAttrs {
		if _, drop := highCardinalityKeys[k]; !drop {
			merged[k] = v
		}
	}
	merged["__temporality__"] = temporality
	merged["__name__"] = metricName

	return FingerprintHash(merged)
}
