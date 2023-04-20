from multiprocessing import Pool, set_start_method

from kloppy.utils import performance_logging

from ingestify.main import get_datastore


def main():
    set_start_method("fork")

    store = get_datastore("config_local.yaml")
    dataset_collection = store.get_dataset_collection(
        provider="wyscout", season_id=188105
    )

    with performance_logging(f"loading data with multiprocessing"):
        dfs = store.map_kloppy(
            lambda dataset, identifier: (dataset.to_df("*", match=identifier.match_id)),
            dataset_collection,
        )

    print(f"Loaded {len(dfs)} matches")

    import pandas as pd

    df = pd.concat(dfs)
    print(df.shape)
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
