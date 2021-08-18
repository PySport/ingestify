from typing import List, Dict

import pandas as pd
from ingestify.domain.models import Dataset, LoadedFile

from kloppy import StatsBombSerializer


class KloppyToPandasTransformer(Transformer):
    def transform(self, dataset: Dataset, loaded_files: Dict[str, LoadedFile]) -> pd.DataFrame:
        if dataset.provider == "statsbomb":
            serializer = StatsBombSerializer()
            kloppy_dataset = serializer.deserialize(
                inputs=dict(
                    event_data=loaded_files['events.json'].stream,
                    lineup_data = loaded_files['lineup.json'].stream,
                ),
                options={}
            )
        else:
            raise Exception(f"Dataset provider {dataset.provider} not known")

        return kloppy_dataset.to_pandas()
