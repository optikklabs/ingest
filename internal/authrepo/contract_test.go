package authrepo

import "testing"

// Pins the cross-repo api-key hash convention shared with the query
// repository's tenant provisioning: api_key_hash = hex(sha256(key)),
// lowercase. If this test fails, the convention changed and every ingest
// API key would be rejected in production — fix the seam on both sides.
func TestAPIKeyHashContract(t *testing.T) {
	const key = "ok_test123"
	const want = "0e783badaac35cd145f1c9292f0c1580e6011760d64b232978e98fb56550f740"
	if got := hashAPIKey(key); got != want {
		t.Fatalf("hashAPIKey(%q) = %s, want %s", key, got, want)
	}
}
