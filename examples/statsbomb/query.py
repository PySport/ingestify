from ingestify.main import get_engine


def main():
    engine = get_engine("config.yaml")

    dataset_collection = engine.get_dataset_collection(
        where="""
        metadata->>'$.home_team.home_team_name' == 'Barcelona' OR 
        metadata->>'$.away_team.away_team_name' == 'Barcelona'
        """
    )
    for dataset in dataset_collection:
        kloppy_dataset = engine.load_with_kloppy(dataset)
        print(f"{kloppy_dataset.metadata.teams[0]} - {kloppy_dataset.metadata.teams[1]}")
        goals = kloppy_dataset.filter("shot.goal")
        for goal in goals:
            print(goal)


if __name__ == "__main__":
    main()
