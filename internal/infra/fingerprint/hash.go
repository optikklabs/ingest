package fingerprint

import (
	"sort"

	"github.com/cespare/xxhash/v2"
)

const separatorByte byte = 255

// FingerprintHash computes a stable xxhash of all attributes
// sorted by key. Sorting guarantees order-independence.
func FingerprintHash(attrs map[string]string) uint64 {
	if len(attrs) == 0 {
		return 0
	}

	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := xxhash.New()
	for _, k := range keys {
		_, _ = h.WriteString(k)
		_, _ = h.Write([]byte{separatorByte})
		_, _ = h.WriteString(attrs[k])
		_, _ = h.Write([]byte{separatorByte})
	}
	return h.Sum64()
}
