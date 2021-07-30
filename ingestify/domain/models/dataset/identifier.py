from .dataset_selector import DatasetSelector


class DatasetIdentifier:
    def __init__(self, dataset_selector: DatasetSelector, **kwargs):
        pass

    @property
    def key(self):
        return ""
