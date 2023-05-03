from multiprocessing import set_start_method

from kloppy.utils import performance_logging

from ingestify.main import get_datastore


def main():
    set_start_method("fork")

    store = get_datastore("config_local.yaml")
    dataset_collection = store.get_dataset_collection(
        provider="wyscout", stage="raw"
    )

    with performance_logging(f"loading data with multiprocessing"):
        dfs = store.map(
            lambda dataset: (
                store.load_with_kloppy(dataset).to_df(
                    "*", match=dataset.identifier.match_id,
                    engine="polars"
                )
            ),
            dataset_collection,
        )

    print(f"Processed {len(dfs)} datasets")


if __name__ == "__main__":
    main()
