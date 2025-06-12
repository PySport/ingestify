# Ingestify Usage Examples

This page provides examples of common usage patterns for Ingestify.

## Basic Data Ingestion

### Command Line

```bash
# Run all ingestion plans
ingestify run --config config.yaml

# Run with a specific provider
ingestify run --config config.yaml --provider statsbomb

# Dry run (without saving data)
ingestify run --config config.yaml --dry-run
```

### Programmatic Usage

```python
from ingestify.main import get_engine

# Initialize the engine
engine = get_engine("config.yaml")

# Run ingestion
engine.load()

# Run for specific provider
engine.load(provider="statsbomb")
```

## Custom Data Source Example

Here's an example of a custom data source for Statsbomb data:

```python
from datetime import datetime
from ingestify import DatasetResource, Source
from ingestify.domain.models.dataset.dataset import DatasetState

class StatsbombMatchAPI(Source):
    """Example data source for Statsbomb match data"""
    
    def __init__(self, name, username, password, base_url="https://api.statsbomb.com/v4"):
        super().__init__(name=name)
        self.username = username
        self.password = password
        self.base_url = base_url
        self.provider = "statsbomb"
    
    def discover_selectors(self, dataset_type: str):
        """Discover available competitions and seasons"""
        assert dataset_type == "match"
        
        competitions = self._get_competitions()
        
        return [
            dict(
                competition_id=competition["competition_id"],
                season_id=competition["season_id"],
                _last_modified=self._get_last_modified(competition)
            )
            for competition in competitions
        ]
    
    def find_datasets(
        self,
        dataset_type: str,
        competition_id: int,
        season_id: int,
        match_id: int = None,
        data_spec_versions=None,
        dataset_collection_metadata=None,
    ):
        """Find match datasets for the given competition and season"""
        assert dataset_type == "match"
        
        # Get match version from data spec versions
        match_version = data_spec_versions.get_version("match")
        
        # Get matches for the competition/season
        matches = self._get_matches(competition_id, season_id)
        
        for match in matches:
            if match_id and match["match_id"] != match_id:
                continue
                
            # Determine dataset state based on match status
            if match["match_status"] == "available" and match["match_status_360"] == "available":
                state = DatasetState.COMPLETE
            elif match["match_status"] == "available":
                state = DatasetState.COMPLETE
            else:
                state = DatasetState.SCHEDULED
                
            # Create match name
            name = (
                f"{match['match_date']} / "
                f"{match['home_team']['home_team_name']} - {match['away_team']['away_team_name']}"
            )
            
            # Create dataset resource
            dataset_resource = DatasetResource(
                dataset_resource_id=dict(
                    competition_id=competition_id,
                    season_id=season_id,
                    match_id=match["match_id"]
                ),
                dataset_type=dataset_type,
                provider=self.provider,
                name=name,
                metadata=match,
                state=state
            )
            
            # Add match data file
            last_modified = datetime.fromisoformat(match["last_updated"] + "+00:00")
            dataset_resource.add_file(
                last_modified=last_modified,
                data_feed_key="match",
                data_spec_version=match_version,
                json_content=match
            )
            
            # Add additional files if match is complete
            if state.is_complete:
                # Update name with score
                name += f" / {match['home_score']}-{match['away_score']}"
                
                # Add events and lineups files
                for data_feed_key in ["lineups", "events"]:
                    for version in data_spec_versions[data_feed_key]:
                        dataset_resource.add_file(
                            last_modified=last_modified,
                            data_feed_key=data_feed_key,
                            data_spec_version=version,
                            url=self._get_url(data_feed_key, version, match["match_id"]),
                            http_options=dict(auth=(self.username, self.password)),
                            data_serialization_format="json"
                        )
                
                # Add 360 data if available
                if match["match_status_360"] == "available" and "360-frames" in data_spec_versions:
                    for version in data_spec_versions["360-frames"]:
                        dataset_resource.add_file(
                            last_modified=datetime.fromisoformat(match["last_updated_360"] + "+00:00"),
                            data_feed_key="360-frames",
                            data_spec_version=version,
                            url=self._get_url("360-frames", version, match["match_id"]),
                            http_options=dict(auth=(self.username, self.password)),
                            data_serialization_format="json"
                        )
            
            yield dataset_resource
    
    def _get_competitions(self):
        """Helper method to fetch competitions"""
        # Implementation would use requests to call the Statsbomb API
        # This is a simplified example
        return [
            {
                "competition_id": 11, 
                "competition_name": "Premier League",
                "season_id": 90,
                "season_name": "2022/2023",
                "match_updated": "2023-05-28T16:30:00",
                "match_updated_360": "2023-05-28T16:35:00"
            }
        ]
    
    def _get_matches(self, competition_id, season_id):
        """Helper method to fetch matches"""
        # Implementation would use requests to call the Statsbomb API
        # This is a simplified example
        return [
            {
                "match_id": 3788741,
                "match_date": "2023-05-28",
                "kick_off": "16:00:00.000",
                "competition": {"competition_id": 11, "competition_name": "Premier League"},
                "season": {"season_id": 90, "season_name": "2022/2023"},
                "home_team": {"home_team_id": 1, "home_team_name": "Arsenal"},
                "away_team": {"away_team_id": 2, "away_team_name": "Chelsea"},
                "home_score": 2,
                "away_score": 1,
                "match_status": "available",
                "match_status_360": "available",
                "last_updated": "2023-05-28T18:00:00",
                "last_updated_360": "2023-05-28T18:05:00"
            }
        ]
    
    def _get_url(self, data_feed_key, version, match_id):
        """Build URL for specific data feed"""
        return f"{self.base_url}/{data_feed_key}/{match_id}"
    
    def _get_last_modified(self, competition):
        """Get last modified timestamp for a competition"""
        if not competition.get("match_updated"):
            return None
            
        last_modified = datetime.fromisoformat(competition["match_updated"] + "+00:00")
        if competition.get("match_updated_360"):
            last_modified_360 = datetime.fromisoformat(competition["match_updated_360"] + "+00:00")
            last_modified = max(last_modified, last_modified_360)
            
        return last_modified
```

## Event Subscriber Example

This example shows how to create a custom event subscriber that processes datasets after they are created or updated:

```python
from ingestify.domain import Dataset
from ingestify.domain.models.event import Subscriber

class StatsbombEventHandler(Subscriber):
    """Example event handler for Statsbomb data"""
    
    def __init__(self, store):
        super().__init__(store)
        # Initialize resources needed for processing
        
    def _process_dataset(self, dataset: Dataset):
        """Process a dataset when it is created or updated"""
        # Skip datasets that aren't from Statsbomb or aren't complete
        if dataset.provider != "statsbomb" or not dataset.state.is_complete:
            return
            
        # Load the dataset files
        files = self.store.load_files(dataset)
        
        # Process different dataset types
        if dataset.dataset_type == "match":
            # Find the events file
            events_file = next((f for f in files if f.data_feed_key == "events"), None)
            if events_file:
                # Process the events data
                events_data = events_file.content
                print(f"Processing {len(events_data)} events for match {dataset.name}")
                
                # Example processing:
                # - Calculate match statistics
                # - Generate event heatmaps
                # - Export to analytics database
                
        elif dataset.dataset_type == "player-season-stats":
            # Process player stats
            stats_file = next((f for f in files if f.data_feed_key == "player-season-stats"), None)
            if stats_file:
                stats_data = stats_file.content
                print(f"Processing season stats for {len(stats_data)} players")
                
                # Example processing:
                # - Aggregate player performance metrics
                # - Generate performance reports
        
    def on_dataset_created(self, event):
        """Called when a new dataset is created"""
        print(f"New dataset created: {event.dataset.name}")
        self._process_dataset(event.dataset)
        
    def on_revision_added(self, event):
        """Called when a new revision is added to an existing dataset"""
        print(f"Revision added to dataset: {event.dataset.name}")
        self._process_dataset(event.dataset)
```

## Configuration Example

Here's a configuration file that demonstrates setting up multiple data sources:

```yaml
main:
  metadata_url: postgresql://user:password@localhost:5432/ingestify
  file_url: s3://sports-data-bucket
  default_bucket: main

sources:
  statsbomb_match:
    type: ingestify.statsbomb_match
    configuration:
      username: !ENV ${STATSBOMB_USERNAME}
      password: !ENV ${STATSBOMB_PASSWORD}
      
  statsbomb_season_stats:
    type: ingestify.statsbomb_season_stats
    configuration:
      username: !ENV ${STATSBOMB_USERNAME}
      password: !ENV ${STATSBOMB_PASSWORD}
      
  wyscout_match:
    type: ingestify.wyscout
    configuration:
      username: !ENV ${WYSCOUT_USERNAME}
      password: !ENV ${WYSCOUT_PASSWORD}
      
  skillcorner_physical:
    type: ingestify.skillcorner
    configuration:
      api_key: !ENV ${SKILLCORNER_API_KEY}


ingestion_plans:
  # Statsbomb match data
  - source: statsbomb_match
    dataset_type: match
    data_spec_versions:
      match: v6
      events: v8
      lineups: v4
    selectors:
      - competition_id: 11  # Premier League
        season_id: [90]     # 2022/2023 season
        
  # Statsbomb 360 data (only for specific competitions)
  - source: statsbomb_match
    dataset_type: match
    data_spec_versions:
      360-frames: v2
    selectors:
      - competition_id: 11  # Premier League

event_subscribers:
  - type: my_package.handlers.StatsbombEventHandler
  - type: my_package.handlers.WyscoutEventHandler
```

## Querying and Accessing Datasets

### Using get_dataset_collection

You can query datasets by passing selector attributes as keyword arguments:

```python
from ingestify.main import get_datastore

# Initialize the datastore
store = get_datastore("config.yaml")

# Get all complete Statsbomb matches for a specific competition and season
datasets = store.get_dataset_collection(
    dataset_state="complete",
    dataset_type="match",
    provider="statsbomb",
    competition_id=11,
    season_id=90
)

print(f"Found {len(datasets)} datasets")

# Access the first dataset
if datasets:
    first_dataset = datasets.first()
    print(f"First dataset: {first_dataset.name}")
    
    # Load files for this dataset
    files = store.load_files(first_dataset)

```

### Using iter_dataset_collection_batches

For large result sets, you can use batches:

```python
# Iterator for datasets with pagination
dataset_collection_batches = engine.iter_datasets(
    dataset_state="complete",
    dataset_type="match",
    provider="statsbomb",
    competition_id=11,
    season_id=90,
    batch_size=100,
    yield_dataset_collection=True
)

# Process each page of results
for batch in dataset_collection_batches:
    print(f"Processing batch with {len(batch)} datasets")
    
    for dataset in batch:
        # Process each dataset
        print(f"Dataset: {dataset.name}")
        
        # Load files if needed
        files = store.load_files(dataset)
        # Process files...
```

## Processing Data with Kloppy

Ingestify integrates well with [Kloppy](https://kloppy.pysport.org/) for sports analytics:

```python
from ingestify.main import get_engine

# Initialize the engine
engine = get_engine("config.yaml")

# Get a specific match dataset
match_dataset = engine.store.get_dataset_collection(
    dataset_type="match",
    provider="statsbomb",
    competition_id=11,     # Premier League
    season_id=90,          # 2022/2023
    match_id=3788741       # Specific match ID
).first()

if match_dataset:
    # Load the dataset with Kloppy
    kloppy_dataset = engine.load_with_kloppy(match_dataset)
    
    # Filter goals
    goals = kloppy_dataset.filter("shot.goal")
    for goal in goals:
        print(f"Goal by {goal.player.name} for {goal.team.name}")
```