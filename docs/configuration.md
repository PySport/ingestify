# Configuration Guide

Ingestify uses YAML configuration files to define data sources, storage locations, and ingestion plans. This guide explains all the configuration options available.

## Basic Structure

A complete configuration file has the following main sections:

```yaml
main:              # Core settings for storage and defaults
sources:           # Data source definitions
dataset_types:     # Dataset type specifications
ingestion_plans:   # Plans for data ingestion
event_subscribers: # Event handlers for post-processing (optional)
```

## Main Section

The `main` section defines core settings for metadata and file storage:

```yaml
main:
  metadata_url: postgresql://user:password@localhost:5432/ingestify
  file_url: s3://sports-data-bucket
  default_bucket: main
```

### Options

- `metadata_url`: Database connection string for metadata storage
  - SQLite: `sqlite:///path/to/database.db`
  - PostgreSQL: `postgresql://user:password@hostname:port/dbname`
  - Environment variables can be used with `!ENV` tag: `!ENV ${DATABASE_URL}`

- `file_url`: Location for storing data files
  - Local filesystem: `file:///path/to/files`
  - S3: `s3://bucket-name`
  - Environment variables can be used with `!ENV` tag: `!ENV s3://bucket-${ENVIRONMENT}`

- `default_bucket`: Default storage bucket name (used if not specified in commands)

## Sources Section

The `sources` section defines the data providers that Ingestify will connect to:

```yaml
sources:
  statsbomb:
    type: ingestify.statsbomb_github
    
  wyscout:
    type: ingestify.wyscout
    configuration:
      username: !ENV ${WYSCOUT_USERNAME}
      password: !ENV ${WYSCOUT_PASSWORD}
      
  custom_source:
    type: my_package.sources.CustomSource
    configuration:
      api_key: !ENV ${API_KEY}
      base_url: https://api.example.com/v1
```

### Options

- `type`: The source class to use
  - Built-in sources:
    - `ingestify.statsbomb_github`: Statsbomb open data from GitHub
    - `ingestify.wyscout`: Wyscout API
  - Custom sources: Full import path to your custom source class

- `configuration`: Source-specific configuration
  - Can include authentication credentials, API endpoints, etc.
  - Sensitive values should use environment variables with the `!ENV` tag
  - Can be a string pointing to a secrets manager: `!ENV vault+aws://path/to/secrets`

## Dataset Types Section

The `dataset_types` section defines how dataset identifiers are handled:

```yaml
dataset_types:
  - provider: statsbomb
    dataset_type: match
    identifier_keys:
      competition_id:
        transformation: str
      season_id:
        transformation: str
      match_id:
        transformation: str
        
  - provider: wyscout
    dataset_type: match
    identifier_keys:
      match_id:
        transformation:
          type: bucket
          bucket_size: 1000
```

### Options

- `provider`: Provider name (must match a source's provider)
- `dataset_type`: Type of dataset (e.g., "match", "player", "team")
- `identifier_keys`: Keys that uniquely identify datasets
  - Each key can have a transformation applied to standardize the format
  - Common transformations:
    - `str`: Convert to string
    - `int`: Convert to integer
    - Bucket transformation:
      ```yaml
      transformation:
        type: bucket
        bucket_size: 1000
      ```

## Ingestion Plans Section

The `ingestion_plans` section defines what data should be fetched:

```yaml
ingestion_plans:
  - source: statsbomb
    dataset_type: match
    data_spec_versions:
      match: v6
      events: v8
      lineups: v4
    selectors:
      - competition_id: 11  # Premier League
        season_id: [90]     # 2022/2023 season
      
  - source: wyscout
    dataset_type: match
    data_spec_versions:
      match: v2
      events: v2
    selectors:
      - competition_id: "EN_PR"
        season_id: "2022"
```

### Options

- `source`: Source name (must match a key in the sources section)
- `dataset_type`: Type of dataset to ingest
- `data_spec_versions`: Versions of different data feeds to request
  - Keys represent data feed keys (e.g., "match", "events", "lineups")
  - Values are version identifiers (provider-specific)
- `selectors`: Filters for what data to fetch
  - Can use specific values, lists, or wildcard expressions
  - Multiple selectors can be specified (processed as OR conditions)
  - For advanced filtering, you can use a string expression: `"*"` or complex conditions

## Event Subscribers Section (Optional)

The `event_subscribers` section defines handlers for processing data after ingestion:

```yaml
event_subscribers:
  - type: my_package.handlers.CustomEventHandler
```

### Options

- `type`: Full import path to the event subscriber class

## Environment Variables and Secrets

Ingestify supports environment variable substitution with the `!ENV` YAML tag:

```yaml
# Simple environment variable
api_key: !ENV ${API_KEY}

# Environment variable with default value
environment: !ENV ${ENVIRONMENT:development}

# Secrets manager reference
database_url: !ENV vault+aws://path/to/secrets/database
```

## Complete Example

Here's a complete configuration example:

```yaml
main:
  metadata_url: !ENV ${DATABASE_URL:sqlite:///database/catalog.db}
  file_url: !ENV ${FILE_URL:file://database/files/}
  default_bucket: main

sources:
  statsbomb:
    type: ingestify.statsbomb_github
  
  wyscout:
    type: ingestify.wyscout
    configuration:
      username: !ENV ${WYSCOUT_USERNAME}
      password: !ENV ${WYSCOUT_PASSWORD}
      base_url: https://api.wyscout.com/v3

dataset_types:
  - provider: statsbomb
    dataset_type: match
    identifier_keys:
      competition_id:
        transformation: int
      season_id:
        transformation: int
      match_id:
        transformation: int
  
  - provider: wyscout
    dataset_type: match
    identifier_keys:
      match_id:
        transformation:
          type: bucket
          bucket_size: 1000

ingestion_plans:
  - source: statsbomb
    dataset_type: match
    data_spec_versions:
      match: v6
      events: v8
      lineups: v4
    selectors:
      - competition_id: 11  # Premier League
        season_id: [90]     # 2022/2023 season
  
  - source: wyscout
    dataset_type: match
    data_spec_versions:
      match: v2
      events: v2
    selectors:
      - competition_id: "EN_PR"  # English Premier League
        season_id: "2022"        # 2022/2023

event_subscribers:
  - type: my_package.handlers.DataProcessingHandler
```