# StatsBomb GitHub Open Data Integration

This guide explains how to use Ingestify to access the open data available from StatsBomb's GitHub repository.

## Overview

StatsBomb provides free, open-source football data via their GitHub repository. This integration allows you to ingest this open data into your Ingestify pipeline without requiring API credentials.

## Configuration

Here's an example configuration for the StatsBomb GitHub open data (all data will be stored in `/tmp/ingestify`):

```yaml
main:
  metadata_url: sqlite:////tmp/ingestify/database.db
  file_url: file:///tmp/ingestify/data
  default_bucket: main

sources:
  statsbomb:
    type: ingestify.statsbomb_github

ingestion_plans:
  # Open data from StatsBomb's GitHub
  - source: statsbomb
    dataset_type: match
    selectors:
      - competition_id: 43  # FIFA World Cup
        season_id: 3
```

## Key Features of the StatsBomb GitHub Integration

### 1. Open Data Access

The integration provides seamless access to StatsBomb's open data:

- No authentication required
- Access to competitions, matches, events, lineups, and 360 data (when available)
- Data is fetched directly from the StatsBomb GitHub repository

### 2. Complete Dataset Support

The integration supports all data types available in the open dataset:

- Match data (basic match information)
- Event data (detailed events during matches)
- Lineup data (team and player information)
- 360 data (when available for specific matches)

### 3. Efficient Caching

The integration efficiently caches data:

- Uses GitHub's raw content URLs for direct access
- Handles data versioning appropriately

## Usage Example

### Command Line Usage

```bash
# Run all ingestion plans
ingestify run --config config.yaml

# Run only StatsBomb GitHub ingestion
ingestify run --config config.yaml --provider statsbomb
```

### Querying for Matches

```python
from ingestify.main import get_engine

# Initialize the engine
engine = get_engine("config.yaml")

# Get all World Cup matches
world_cup_matches = engine.store.get_dataset_collection(
    provider="statsbomb",
    dataset_type="match",
    competition_id=43,
    season_id=3,
    dataset_state="complete"
)

# Get a specific match
match = engine.store.get_dataset_collection(
    provider="statsbomb",
    dataset_type="match",
    competition_id=43,
    season_id=3,
    match_id=8658
).first()
```

## Available Open Data

StatsBomb's open data includes:

- FIFA World Cup 2018
- FIFA Women's World Cup 2019
- UEFA Euro 2020
- NWSL 2018
- Selected FAWSL matches
- Selected La Liga matches

Check the [StatsBomb Open Data repository](https://github.com/statsbomb/open-data) for the latest available competitions and seasons.