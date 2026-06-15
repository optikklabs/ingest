package fingerprint

import "sort"

const (
	offset64      uint64 = 14695981039346656037
	prime64       uint64 = 1099511628211
	separatorByte byte   = 255
)

// hashAdd feeds a string into an FNV-64a hash, returning the updated state.
func hashAdd(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashAddByte feeds a single byte into an FNV-64a hash.
func hashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= prime64
	return h
}

// FingerprintHash computes a stable FNV-64a hash of all resource attributes
// sorted by key. Sorting guarantees order-independence.
func FingerprintHash(attrs map[string]string) uint64 {
	if len(attrs) == 0 {
		return offset64
	}

	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sum := offset64
	for _, k := range keys {
		sum = hashAdd(sum, k)
		sum = hashAddByte(sum, separatorByte)
		sum = hashAdd(sum, attrs[k])
		sum = hashAddByte(sum, separatorByte)
	}
	return sum
}
