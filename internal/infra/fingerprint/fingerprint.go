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

var highCardinalityKeys = map[string]struct{}{
	"k8s.pod.uid":        {},
	"k8s.replicaset.uid": {},
	"container.id":       {},
	"process.pid":        {},
}

func SeriesHash(metricName, temporality string, resAttrs, dpAttrs map[string]string) uint64 {
	return SeriesHashPreFiltered(metricName, temporality, FilterHighCardinality(resAttrs), dpAttrs)
}

// FilterHighCardinality returns a copy of attrs without the high-cardinality
// keys. Hoist this once per ResourceMetrics instead of per datapoint.
func FilterHighCardinality(attrs map[string]string) map[string]string {
	out := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if _, drop := highCardinalityKeys[k]; !drop {
			out[k] = v
		}
	}
	return out
}

// SeriesHashPreFiltered hashes a series whose resource attrs are already
// high-cardinality-filtered; only dpAttrs is filtered here (per datapoint).
func SeriesHashPreFiltered(metricName, temporality string, filteredResAttrs, dpAttrs map[string]string) uint64 {
	merged := make(map[string]string, len(filteredResAttrs)+len(dpAttrs)+2)
	for k, v := range filteredResAttrs {
		merged[k] = v
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
