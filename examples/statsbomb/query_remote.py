from ingestify.main import get_remote_datastore


def main():
    store = get_remote_datastore("teamtv://PSV 1", bucket="main2")

    dataset_collection = store.get_dataset_collection(
        where="""
        metadata->>'$.home_team.home_team_name' = 'Barcelona' OR 
        metadata->>'$.away_team.away_team_name' = 'Barcelona'
        """
    )
    for dataset in dataset_collection:
        kloppy_dataset = store.load_with_kloppy(dataset)
        print(
            f"{kloppy_dataset.metadata.teams[0]} - {kloppy_dataset.metadata.teams[1]}"
        )
        goals = kloppy_dataset.filter("shot.goal")
        for goal in goals:
            print(goal)


if __name__ == "__main__":
    main()
