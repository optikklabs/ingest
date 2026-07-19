package fingerprint

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
		Service:     firstValue(attrs, serviceNameLabels...),
		Host:        firstValue(attrs, hostLabels...),
		Pod:         firstValue(attrs, "k8s.pod.name", "k8s.pod.uid"),
		Container:   firstValue(attrs, "k8s.container.name", "container.name", "container_name"),
		Environment: firstValue(attrs, "deployment.environment", "env"),
		Version:     firstValue(attrs, "service.version", "version"),
		Namespace:   firstValue(attrs, namespaceLabels...),
	}
}

func firstValue(attrs map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := attrs[key]; value != "" {
			return value
		}
	}
	return ""
}
