package fingerprint

import "testing"

func TestResolveResourceUsesFingerprintAliases(t *testing.T) {
	dims := ResolveResource(map[string]string{
		"k8s.deployment.name": "checkout",
		"env":                 "prod",
		"version":             "v2",
		"host":                "node-1",
	})
	if dims.Service != "checkout" || dims.Environment != "prod" ||
		dims.Version != "v2" || dims.Host != "node-1" {
		t.Fatalf("dimensions = %+v", dims)
	}
}

func TestResolveResourcePrefersCanonicalKeys(t *testing.T) {
	dims := ResolveResource(map[string]string{
		"service.name":           "canonical",
		"k8s.deployment.name":    "fallback",
		"deployment.environment": "production",
		"env":                    "prod",
	})
	if dims.Service != "canonical" || dims.Environment != "production" {
		t.Fatalf("dimensions = %+v", dims)
	}
}
