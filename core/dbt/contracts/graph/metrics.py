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

    def __init__(self, node, manifest, Relation):
        super().__init__(node.name, node.package_name)
        self.node = node
        self.manifest = manifest
        self.Relation = Relation

    def __getattr__(self, key):
        return getattr(self.node, key)

    def __str__(self):
        return self.node.name

    def parent_metrics(self):
        if self.node.type != 'ratio':
            return []

        ratio_terms = self.node.ratio_terms

        numerator_id = ratio_terms.numerator.unique_id
        denominator_id = ratio_terms.denominator.unique_id

        return [
            self.manifest.metrics.get(numerator_id),
            self.manifest.metrics.get(denominator_id),
        ]

    def parent_models(self):
        search_metrics = [self.node] + self.parent_metrics()

        # We need to figure out how to get a handle on the model
        # that powers this metric. We can use `refs`, but that sucks!
        # We should try to pop a unique_id somewhere instead?
        # actually....
        # We can iterate over metric.depends_on and look up nodes in the manifest?

        for metric in search_metrics:
            import ipdb; ipdb.set_trace()
            for depends_on in metric.depends_on.nodes:
                pass

            node = self.manifest.ref_lookup.find(target_model_name, pkg, self)
            # rel = self.Relation.create(
            pass

