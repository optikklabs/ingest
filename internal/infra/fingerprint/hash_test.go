package fingerprint

import "testing"

// Fingerprints are stored; the hash of a given attribute set must never change.
func TestFingerprintHashIsStable(t *testing.T) {
	attrs := map[string]string{"service.name": "api", "host.name": "h1", "env": "prod"}
	if got := FingerprintHash(attrs); got != 8526699836984459243 {
		t.Fatalf("FingerprintHash = %d, want 8526699836984459243", got)
	}
	if FingerprintHash(nil) != 0 {
		t.Fatal("empty attribute set must hash to 0")
	}
}
