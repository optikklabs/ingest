package fingerprint

import (
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

var highCardinalityKeys = map[string]struct{}{
	"k8s.pod.uid":        {},
	"k8s.replicaset.uid": {},
	"container.id":       {},
	"process.pid":        {},
}

func FilterHighCardinality(attrs map[string]string) map[string]string {
	out := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if _, drop := highCardinalityKeys[k]; !drop {
			out[k] = v
		}
	}
	return out
}

var seriesKeyScratch = sync.Pool{
	New: func() any { s := make([]string, 0, 48); return &s },
}

// SeriesHashPreFiltered hashes the merged series identity without building a
// per-datapoint map: keys are gathered into a pooled scratch slice, sorted,
// and deduped adjacently. The byte stream fed to xxhash is identical to
// FingerprintHash over the old merged map (see the equivalence test);
// fingerprints persist in metrics_series, so this must never change.
func SeriesHashPreFiltered(metricName, temporality string, filteredResAttrs, dpAttrs map[string]string) uint64 {
	scratch := seriesKeyScratch.Get().(*[]string)
	keys := (*scratch)[:0]
	for k := range filteredResAttrs {
		keys = append(keys, k)
	}
	for k := range dpAttrs {
		if _, drop := highCardinalityKeys[k]; !drop {
			keys = append(keys, k)
		}
	}
	keys = append(keys, "__temporality__", "__name__")
	sort.Strings(keys)

	var h xxhash.Digest
	h.Reset()
	prev := ""
	for i, k := range keys {
		if i > 0 && k == prev {
			continue
		}
		prev = k
		_, _ = h.WriteString(k)
		_, _ = h.Write(separator)
		_, _ = h.WriteString(seriesValue(k, metricName, temporality, filteredResAttrs, dpAttrs))
		_, _ = h.Write(separator)
	}
	*scratch = keys
	seriesKeyScratch.Put(scratch)
	return h.Sum64()
}

// seriesValue mirrors merged-map precedence: sentinels win, then datapoint
// attrs (unless high-cardinality), then resource attrs.
func seriesValue(k, metricName, temporality string, resAttrs, dpAttrs map[string]string) string {
	switch k {
	case "__name__":
		return metricName
	case "__temporality__":
		return temporality
	}
	if v, ok := dpAttrs[k]; ok {
		if _, drop := highCardinalityKeys[k]; !drop {
			return v
		}
	}
	return resAttrs[k]
}
