# Core Concepts

This guide explains the fundamental concepts and architecture of Ingestify.

## Overview

Ingestify is a framework for ingesting, storing, and managing sports analytics datasets from various providers. It follows a domain-driven design approach with clear separation between:

- **Domain layer**: Core entities and business logic
- **Application layer**: Orchestration and use cases
- **Infrastructure layer**: Implementation details (storage, networking, etc.)

## Key Components

### Sources

Sources are data providers that Ingestify can fetch data from. Examples include Statsbomb, Wyscout, and Skillcorner. Each source has its own API structure, authentication, and data formats.

A source is responsible for:
- Discovering available data (competitions, seasons, matches, etc.)
- Fetching data for specific selectors
- Converting provider-specific data to Ingestify's domain model

### Datasets

A dataset is a logical collection of related data files. For example, a match dataset might include:
- Match metadata
- Event data
- Lineup information
- Tracking data

Each dataset has:
- A unique identifier
- A provider (e.g., "statsbomb")
- A dataset type (e.g., "match")
- A state (COMPLETE, PARTIAL, SCHEDULED)
- Resource identifiers (e.g., competition_id, season_id, match_id)
- Metadata
- One or more revisions (versions)

### Files

Files are the actual data components of a dataset. Each file:
- Belongs to a specific dataset and revision
- Has a data feed key (e.g., "match", "events", "lineups")
- Has a data specification version
- Contains the actual data content

### Revisions

Revisions represent versions of a dataset over time. When data is updated, a new revision is created rather than overwriting the existing data. This provides:
- Data history
- Version tracking
- The ability to work with consistent snapshots

### DatasetStore

The DatasetStore manages the storage and retrieval of datasets and files. It consists of:
- A DatasetRepository for storing dataset metadata
- A FileRepository for storing file content

Ingestify supports multiple storage backends:
- Local filesystem
- S3
- Various database systems for metadata (SQLite, PostgreSQL)

### IngestionEngine

The IngestionEngine orchestrates the data ingestion process:
1. Reads configuration to determine what data to fetch
2. Connects to sources to discover and fetch datasets
3. Compares new data with existing data to determine what needs to be updated
4. Stores new or updated datasets and files
5. Triggers events for post-processing

### Event System

Ingestify includes an event-based architecture for processing data after ingestion:
- Events are triggered when datasets are created or updated
- Event subscribers can react to these events to perform additional processing
- This allows for decoupled, extensible data pipelines

## Data Flow

1. **Configuration**: Define what data to ingest in a YAML file
2. **Discovery**: Sources discover available data based on selectors
3. **Fetching**: Data is fetched from sources
4. **Processing**: Data is processed and normalized
5. **Storage**: Datasets and files are stored in repositories
6. **Events**: Events are triggered for post-processing
7. **Access**: Data can be retrieved through the DatasetStore API

## Identifiers and Transformations

Each dataset has a set of resource identifiers that uniquely identify it. For example, a match dataset might be identified by:
- competition_id
- season_id
- match_id

These identifiers can have transformations applied to standardize their format:
- String transformations
- Integer transformations
- Bucket transformations (for efficient storage)

## Data Specification Versions

Data from sources evolves over time, with schema changes and new fields. Ingestify handles this through data specification versions:
- Each data feed (match, events, lineups) can have a specific version
- This allows for backward compatibility
- Sources can provide data in multiple versions

## FetchPolicy

The FetchPolicy determines when data should be re-fetched:
- Based on last modified timestamps
- By comparing data content
- According to specific rules (e.g., always re-fetch incomplete datasets)

## Selectors

Selectors specify what data to fetch from sources:
- Simple key-value pairs (e.g., competition_id=11, season_id=90)
- Lists for multiple values (e.g., season_id=[90, 91])
- Wildcard expressions for advanced filtering

## Domain-Driven Design

Ingestify's architecture follows domain-driven design principles:
- **Entities**: Datasets, Files, Revisions
- **Value Objects**: DatasetState, Identifiers
- **Repositories**: DatasetRepository, FileRepository
- **Services**: Transformers, Factories
- **Events**: DomainEvents for data changes
- **Aggregates**: Dataset as the root entity

This approach provides a clear separation of concerns and a focus on the business domain rather than technical details.