package fingerprint

// highCardinalityKeys are attribute keys excluded from series identity to cap
// cardinality, mirroring collector-side label drops. Dropping a key collapses
// series that differ only by it, so their rollup values merge.
var highCardinalityKeys = map[string]struct{}{
	"k8s.pod.uid":        {},
	"k8s.replicaset.uid": {},
	"container.id":       {},
	"process.pid":        {},
}

// SeriesHash is the full time-series identity for metrics: resource and
// data-point attributes flatten into one map (high-cardinality keys dropped),
// plus "__name__" and "__temporality__" sentinels, hashed by FingerprintHash.
// Fingerprints persist in metrics_series, so the bytes must never change.
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
