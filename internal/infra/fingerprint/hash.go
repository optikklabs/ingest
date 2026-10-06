package fingerprint

import (
	"maps"
	"slices"

	"github.com/cespare/xxhash/v2"
)

const separatorByte byte = 255

var separator = []byte{separatorByte}

func FingerprintHash(attrs map[string]string) uint64 {
	if len(attrs) == 0 {
		return 0
	}

	h := xxhash.New()
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		_, _ = h.WriteString(k)
		_, _ = h.Write(separator)
		_, _ = h.WriteString(attrs[k])
		_, _ = h.Write(separator)
	}
	return h.Sum64()
}
