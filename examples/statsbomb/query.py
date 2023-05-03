from multiprocessing import Pool, set_start_method

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

    print(f"Exported {len(dataset_collection)} matches")
    #
    # import pandas as pd
    #
    # df = pd.concat(dfs)
    # print(df.shape)
    #
    # return
    # dataset_collection = store.get_dataset_collection(
    #     where="""
    #     metadata->>'$.home_team.home_team_name' = 'Barcelona' OR
    #     metadata->>'$.away_team.away_team_name' = 'Barcelona'
    #     """
    # )
    #
    # dfs = []
    # for dataset in dataset_collection:
    #     kloppy_dataset = store.load_with_kloppy(dataset)

    # print(f"{kloppy_dataset.metadata.teams[0]} - {kloppy_dataset.metadata.teams[1]}")
    # goals = kloppy_dataset.filter("shot.goal")
    # for goal in goals:
    #    print(goal)


if __name__ == "__main__":
    main()
