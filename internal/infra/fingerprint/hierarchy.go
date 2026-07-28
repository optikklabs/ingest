package fingerprint

var (
	namespaceLabels = []string{
		"service.namespace",
		"k8s.namespace.name",
	}
	serviceNameLabels = []string{
		"service.name",
		"cloudwatch.log.group.name",
		"k8s.deployment.name",
		"k8s.deployment.uid",
		"k8s.statefulset.name",
		"k8s.statefulset.uid",
		"k8s.daemonset.name",
		"k8s.daemonset.uid",
		"k8s.job.name",
		"k8s.job.uid",
		"k8s.cronjob.name",
		"k8s.cronjob.uid",
		"faas.name",
	}
	hostLabels = []string{
		"k8s.node.name",
		"k8s.node.uid",
		"host.id",
		"host.name",
		"host.ip",
		"host",
	}
)
