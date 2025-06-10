# StatsBomb API Integration

This guide explains how to integrate with the StatsBomb API using Ingestify, based on a production implementation.

## Configuration

Here's an example configuration for the StatsBomb API:

```yaml
main:
  metadata_url: postgresql://user:password@localhost:5432/ingestify
  file_url: s3://sports-data-bucket
  default_bucket: main

sources:
  statsbomb_match:
    type: ingestify.infra.source.statsbomb.StatsBombMatchAPI
    configuration:
      username: !ENV ${STATSBOMB_USERNAME}
      password: !ENV ${STATSBOMB_PASSWORD}

ingestion_plans:
  # Standard match data
  - source: statsbomb_match
    dataset_type: match
    data_spec_versions:
      match: v6
      events: v8
      lineups: v4
    selectors:
      - competition_id: 11  # Premier League
        season_id: [90]     # 2022/2023 season
        
  # 360 data (only for specific competitions)
  - source: statsbomb_match
    dataset_type: match
    data_spec_versions:
      360-frames: v2
    selectors:
      - competition_id: 11  # Premier League
```

## Key Benefits of the StatsBomb Integration

### 1. Efficient Data Fetching

The StatsBomb integration is optimized for performance:

- Uses timestamp information at the competition level to skip fetching unchanged data
- Only attempts to fetch data that's actually available (e.g., 360 data only when marked as available)
- This minimizes API calls and speeds up repeated ingestion runs

### 2. Multiple Data Specification Versions

The integration supports StatsBomb's versioned data formats:

- Different versions for different data types (match, events, lineups, 360-frames)
- Allows for granular control over which data feeds to ingest
- Can be configured to fetch only the data feeds you need

### 3. Separate Ingestion Plans

A key practice demonstrated is using separate ingestion plans:

- One plan for standard match data (match, events, lineups)
- Another plan specifically for 360 data
- This allows fetching 360 data only for specific competitions where it's available

### 4. Proper State Handling

The integration properly maps StatsBomb's status fields to Ingestify's dataset states:

- Matches marked as "Complete" and "available" become COMPLETE datasets
- Matches that are "Complete" but still "processing" become PARTIAL datasets
- Other matches become SCHEDULED datasets
- This gives you accurate information about data completeness

## Usage Example

### Command Line Usage

```bash
# Run all ingestion plans
ingestify run --config config.yaml

# Run only StatsBomb ingestion
ingestify run --config config.yaml --provider statsbomb
```

### Querying for Matches

```python

```python
from ingestify.main import get_engine

# Initialize the engine
engine = get_engine("config.yaml")

# Get all Premier League matches
premier_league_matches = engine.store.get_dataset_collection(
    provider="statsbomb",
    dataset_type="match",
    competition_id=11,
    season_id=90,
    dataset_state="complete"
)

# Get a specific match
match = engine.store.get_dataset_collection(
    provider="statsbomb",
    dataset_type="match",
    competition_id=11,
    season_id=90,
    match_id=3788741
).first()
```

### Using with Kloppy

```python
# Load match with Kloppy for advanced analysis
kloppy_dataset = engine.load_with_kloppy(match)

# Filter for specific events
shots = kloppy_dataset.filter("shot")

# Check https://kloppy.pysport.org/user-guide/concepts/event-data/#kloppys-event-data-model
for shot in shots:
    process(shot.freeze_frame)
```