# Getting Started with Ingestify

This guide will help you get started with Ingestify, a Python framework for ingesting and managing sports analytics datasets.

## Installation

Install Ingestify using pip:

```bash
pip install ingestify
```

## Basic Setup

Ingestify requires a configuration file to define your data sources and ingestion plans. Create a file named `config.yaml` in your project directory.

### Minimal Configuration Example

```yaml
main:
  metadata_url: sqlite:///database/catalog.db  # SQLite database for metadata
  file_url: file://database/files/  # Local file storage
  default_bucket: main  # Default storage bucket

sources:
  statsbomb:  # Define a Statsbomb data source
    type: ingestify.statsbomb_github

ingestion_plans:
  - source: statsbomb  # Use the Statsbomb source defined above
    dataset_type: match
    selectors:
      - competition_id: 11
        season_id: [90]
```

## Running Your First Ingestion

1. Create the necessary directories:

```bash
mkdir -p database/files
```

2. Run the ingestify command:

```bash
ingestify run --config config.yaml
```

This will:
- Connect to the configured data source
- Download data according to your ingestion plans
- Store metadata in the specified database
- Save files to the defined file storage location

## Checking Your Data

List the ingested datasets:

```bash
ingestify list --config config.yaml
```

## Advanced Setup

For production environments, you might want to use more advanced configurations:

### Using Environment Variables

```yaml
main:
  metadata_url: !ENV ${DATABASE_URL}
  file_url: !ENV s3://my-data-bucket-${ENVIRONMENT:production}
  default_bucket: main
```

### Multiple Data Sources

```yaml
sources:
  match_data:
    type: custom.sources.MatchDataAPI
    configuration: !ENV ${MATCH_API_CONFIG}
  
  player_stats:
    type: custom.sources.PlayerStatsAPI
    configuration: !ENV ${PLAYER_API_CONFIG}
```

## Next Steps

- Learn about [configuration options](./configuration.md)
- Explore [CLI commands](./cli_commands.md)
- Understand [core concepts](./core_concepts.md)
- Check out [examples](./examples.md) for more advanced usage