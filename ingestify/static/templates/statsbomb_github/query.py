from ingestify.main import get_datastore


def main():
    store = get_datastore("config.yaml")
    dataset_collection = store.get_dataset_collection()

    for dataset in dataset_collection:
        kloppy_dataset = store.load_with_kloppy(dataset)
        print(f"Loaded dataset with {len(kloppy_dataset.records)} events")


if __name__ == "__main__":
    main()
