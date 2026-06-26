package fingerprint

// DimensionHierarchyNode represents a level in the resource identity tree.
type DimensionHierarchyNode struct {
	labels         []string
	subHierarchies []DimensionHierarchyNode
}

type IdLabelValue struct {
	Label string
	Value string
}

func (n *DimensionHierarchyNode) Identifier(attrs map[string]string) []IdLabelValue {
	var result []IdLabelValue
	for _, l := range n.labels {
		if v, ok := attrs[l]; ok && v != "" {
			result = append(result, IdLabelValue{Label: l, Value: v})
			break
		}
	}
	for _, sub := range n.subHierarchies {
		if subLabels := sub.Identifier(attrs); len(subLabels) > 0 {
			result = append(result, subLabels...)
			break
		}
	}
	return result
}

func ResourceHierarchy() *DimensionHierarchyNode {
	return defaultRoot
}

var defaultRoot = buildRoot()

func buildRoot() *DimensionHierarchyNode {
	return cloudProviderLevel()
}

func cloudProviderLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"cloud.provider"},
		subHierarchies: []DimensionHierarchyNode{*cloudAccountLevel()},
	}
}

func cloudAccountLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"cloud.account.id"},
		subHierarchies: []DimensionHierarchyNode{*gcpProjectLevel()},
	}
}

func gcpProjectLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"gcp.project"},
		subHierarchies: []DimensionHierarchyNode{*regionLevel()},
	}
}

func regionLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"cloud.region", "aws.region"},
		subHierarchies: []DimensionHierarchyNode{*platformLevel()},
	}
}

func platformLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"cloud.platform", "source_type"},
		subHierarchies: []DimensionHierarchyNode{*clusterLevel()},
	}
}

func clusterLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels: []string{
			"k8s.cluster.name",
			"k8s.cluster.uid",
			"aws.ecs.cluster.arn",
		},
		subHierarchies: []DimensionHierarchyNode{
			*serviceView(),
			*nodeView(),
		},
	}
}

func serviceView() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         namespaceLabels,
		subHierarchies: []DimensionHierarchyNode{*serviceNameLevel()},
	}
}

func serviceNameLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         serviceNameLabels,
		subHierarchies: []DimensionHierarchyNode{*environmentLevel()},
	}
}

func environmentLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"deployment.environment", "env"},
		subHierarchies: []DimensionHierarchyNode{*versionLevel()},
	}
}

func versionLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"service.version", "version"},
		subHierarchies: []DimensionHierarchyNode{*instanceLevel()},
	}
}

func instanceLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         instanceLabels,
		subHierarchies: []DimensionHierarchyNode{*containerLevel()},
	}
}

func containerLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels: []string{
			"k8s.container.name",
			"container.name",
			"container_name",
		},
		subHierarchies: []DimensionHierarchyNode{{labels: []string{"component"}}},
	}
}

func nodeView() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         []string{"cloud.availability_zone"},
		subHierarchies: []DimensionHierarchyNode{*nodeLevel()},
	}
}

func nodeLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels:         hostLabels,
		subHierarchies: []DimensionHierarchyNode{*nodePodLevel()},
	}
}

func nodePodLevel() *DimensionHierarchyNode {
	return &DimensionHierarchyNode{
		labels: []string{
			"k8s.pod.name",
			"k8s.pod.uid",
			"aws.ecs.task.id",
			"aws.ecs.task.arn",
		},
		subHierarchies: []DimensionHierarchyNode{{
			labels: []string{"k8s.container.name", "container.name"},
		}},
	}
}

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
	instanceLabels = []string{
		"service.instance.id",
		"k8s.pod.name",
		"k8s.pod.uid",
		"aws.ecs.task.id",
		"aws.ecs.task.arn",
		"cloudwatch.log.stream",
		"cloud.resource_id",
		"faas.instance",
		"host.id",
		"host.name",
		"host.ip",
		"host",
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
