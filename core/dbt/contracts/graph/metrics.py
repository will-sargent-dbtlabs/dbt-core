class MetricReference(object):
    def __init__(self, metric_name, package_name=None):
        self.metric_name = metric_name
        self.package_name = package_name


class ResolvedMetricReference(MetricReference):
    """
    Simple proxy over a ParsedMetric which delegates property
    lookups to the underlying node. Also adds helper functions
    for working with metrics (ie. __str__ and templating functions)
    """

    def __init__(self, node):
        super().__init__(node.name, node.package_name)
        self.node = node

    def __getattr__(self, key):
        return getattr(self.node, key)

    @property
    def is_derived(self):
        return self.node.is_derived

    def qualify(self, namespace):
        return f"{namespace}.{self.metric_name}"

    def __str__(self):
        return self.node.name
