# API Reference

This guide covers the main classes and functions of the Ingestify API for programmatic usage.

## Main Components

### Engine Initialization

```python
from ingestify.main import get_engine, get_datastore

# Initialize the engine with a configuration file
engine = get_engine("config.yaml")

# Initialize just the datastore
store = get_datastore("config.yaml")

# Specify a bucket
engine = get_engine("config.yaml", bucket="production")

# Disable events
engine = get_engine("config.yaml", disable_events=True)
```

### IngestionEngine

The main class for running ingestion jobs.

```python
# Run all ingestion plans
engine.load()

# Run with filters
engine.load(
    dry_run=False,      # Set to True to run without storing data
    provider="statsbomb",  # Filter by provider
    source="statsbomb_match"  # Filter by source
)

# List all datasets
engine.list_datasets(as_count=False)

# Delete a dataset
engine.destroy_dataset(dataset_id="3f7d8e9a-1234-5678-90ab-cdef01234567")
engine.destroy_dataset(provider="statsbomb", dataset_type="match", match_id=3788741)
```

### DatasetStore

The main class for working with stored datasets.

```python
# Get a collection of datasets
datasets = engine.store.get_dataset_collection(
    provider="statsbomb",
    dataset_type="match",
    competition_id=11,
    season_id=90,
    dataset_state="complete"
)

# Get a specific dataset
dataset = datasets.first()

# Load files for a dataset
files = engine.store.load_files(dataset)

# Load files lazily (only metadata, content loaded on demand)
files = engine.store.load_files(dataset, lazy=True)

# Access file content
match_file = next((f for f in files if f.data_feed_key == "match"), None)
events_file = next((f for f in files if f.data_feed_key == "events"), None)

if match_file:
    match_data = match_file.content
    
if events_file:
    events_data = events_file.content
```

### Pagination

For large result sets, use pagination:

```python
# Iterator for datasets with pagination
dataset_collection_batches = store.iter_dataset_collection_batches(
    provider="statsbomb",
    dataset_type="match",
    competition_id=11,
    season_id=90,
    page_size=100,
    yield_dataset_collection=True
)

# Process each page of results
for batch in dataset_collection_batches:
    for dataset in batch:
        # Process each dataset
        files = store.load_files(dataset)
        # ...
```

## Data Models

### Dataset

Represents a dataset in the store.

```python
# Properties
dataset.dataset_id          # Unique identifier
dataset.provider            # Data provider (e.g., "statsbomb")
dataset.dataset_type        # Type of dataset (e.g., "match")
dataset.name                # Human-readable name
dataset.state               # DatasetState (COMPLETE, PARTIAL, SCHEDULED)
dataset.created_at          # Creation timestamp
dataset.last_modified_at    # Last modification timestamp
dataset.revision_id         # Current revision ID
dataset.dataset_resource_id # Dictionary of dataset keys
```

### DatasetState

Enum representing the state of a dataset.

```python
from ingestify.domain.models.dataset.dataset_state import DatasetState

# States
DatasetState.COMPLETE  # Dataset is complete
DatasetState.PARTIAL   # Dataset is partially complete
DatasetState.SCHEDULED  # Dataset is scheduled but not yet available

# Check if a dataset is complete
if dataset.state.is_complete:
    # Process complete dataset
    pass
```

### File

Represents a file in a dataset.

```python
# Properties
file.file_id           # Unique identifier
file.dataset_id        # Associated dataset ID
file.revision_id       # Revision ID
file.data_feed_key     # Type of data (e.g., "match", "events")
file.data_spec_version # Version of the data specification
file.file_path         # Path to the file in storage
file.content           # File content (loaded when accessed)
```

## Creating Custom Components

### Custom Source

To create a custom data source, extend the `Source` class:

```python
from datetime import datetime
from ingestify import DatasetResource, Source
from ingestify.domain.models.dataset.dataset import DatasetState

class CustomSource(Source):
    def __init__(self, name, api_key, base_url="https://api.example.com"):
        super().__init__(name=name)
        self.api_key = api_key
        self.base_url = base_url
        self.provider = "custom_provider"
    
    def discover_selectors(self, dataset_type: str):
        """Return available selectors for this source"""
        # Implementation specific to your data source
        return [
            {
                "competition_id": "1",
                "season_id": "2023",
                "_last_modified": datetime.now()
            }
        ]
    
    def find_datasets(
        self,
        dataset_type: str,
        competition_id: str,
        season_id: str,
        match_id: str = None,
        data_spec_versions=None,
        dataset_collection_metadata=None,
    ):
        """Find datasets matching the selectors"""
        # Implementation specific to your data source
        
        # Create and yield dataset resources
        dataset_resource = DatasetResource(
            dataset_resource_id={"competition_id": competition_id, "season_id": season_id, "match_id": "12345"},
            dataset_type=dataset_type,
            provider=self.provider,
            name="Example match",
            metadata={"some": "metadata"},
            state=DatasetState.COMPLETE
        )
        
        # Add files to the dataset resource
        dataset_resource.add_file(
            last_modified=datetime.now(),
            data_feed_key="match",
            data_spec_version="v1",
            json_content={"match": "data"}
        )
        
        yield dataset_resource
```

### Custom Event Subscriber

To create a custom event subscriber, extend the `Subscriber` class:

```python
from ingestify.domain import Dataset
from ingestify.domain.models.event import Subscriber

class CustomEventHandler(Subscriber):
    def __init__(self, store):
        super().__init__(store)
        # Initialize any resources needed
    
    def on_dataset_created(self, event):
        """Called when a new dataset is created"""
        dataset = event.dataset
        # Process the new dataset
        
    def on_revision_added(self, event):
        """Called when a revision is added to a dataset"""
        dataset = event.dataset
        # Process the updated dataset
```

## Kloppy Integration

Ingestify provides integration with [Kloppy](https://kloppy.pysport.org/) for sports analytics:

```python
# Load a dataset with Kloppy
kloppy_dataset = engine.load_with_kloppy(dataset)

# Use Kloppy's filtering and transformation features
shots = kloppy_dataset.filter("shot")
goals = kloppy_dataset.filter("shot.goal")

# Access event data
for event in shots:
    print(f"Shot by {event.player.name} at {event.timestamp}")

# Access tracking data (if available)
for frame in kloppy_dataset.frames:
    # Process tracking frame
    pass
```