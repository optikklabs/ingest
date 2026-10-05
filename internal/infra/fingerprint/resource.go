package fingerprint

import "github.com/optikklabs/ingest/internal/infra/otlp"

type ResourceDimensions struct {
	Service     string
	Host        string
	Pod         string
	Container   string
	Environment string
	Version     string
	Namespace   string
}

func ResolveResource(attrs map[string]string) ResourceDimensions {
	return ResourceDimensions{
		Service:     otlp.FirstNonEmpty(attrs, serviceNameLabels...),
		Host:        otlp.FirstNonEmpty(attrs, hostLabels...),
		Pod:         otlp.FirstNonEmpty(attrs, "k8s.pod.name", "k8s.pod.uid"),
		Container:   otlp.FirstNonEmpty(attrs, "k8s.container.name", "container.name", "container_name"),
		Environment: otlp.FirstNonEmpty(attrs, "deployment.environment", "env"),
		Version:     otlp.FirstNonEmpty(attrs, "service.version", "version"),
		Namespace:   otlp.FirstNonEmpty(attrs, namespaceLabels...),
	}
}
