from .selector import DatasetSelector


class DatasetIdentifier:
    def __init__(self, dataset_selector: DatasetSelector, **kwargs):
        self.dataset_selector = dataset_selector
        self.attributes = kwargs

    def __getattr__(self, item):
        return self.attributes[item]

    @property
    def key(self):
        return ""

    def __repr__(self):
        attributes = {k: v for k, v in self.attributes.items() if not k.startswith('_')}
        return f"DatasetIdentifier(dataset_selector={self.dataset_selector}, attributes={attributes})"
