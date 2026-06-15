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
