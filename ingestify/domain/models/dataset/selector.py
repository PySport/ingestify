from ingestify.utils import AttributeBag


class Selector(AttributeBag):
    def __bool__(self):
        return len(self.filtered_attributes) > 0
