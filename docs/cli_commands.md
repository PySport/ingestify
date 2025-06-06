# Command Line Interface

Ingestify provides a command-line interface (CLI) that allows you to manage datasets and run ingestion jobs. This guide covers all available commands and their options.

## Basic Usage

```bash
ingestify COMMAND [OPTIONS]
```

All commands accept the `--config` option to specify the configuration file:

```bash
ingestify COMMAND --config config.yaml
```

If not specified, Ingestify will look for a file named `config.yaml` in the current directory or use the path from the `INGESTIFY_CONFIG_FILE` environment variable.

## Available Commands

### run

Run ingestion jobs to fetch and store data.

```bash
ingestify run [OPTIONS]
```

#### Options

- `--config FILE`: Path to the configuration file
- `--bucket BUCKET`: Storage bucket to use (overrides the default_bucket in config)
- `--debug`: Enable debug logging
- `--dry-run`: Execute without storing any data (useful for testing)
- `--provider PROVIDER`: Only run tasks for a specific provider
- `--source SOURCE`: Only run tasks for a specific source
- `--disable-events`: Disable all event handlers

#### Examples

```bash
# Run all ingestion plans
ingestify run --config config.yaml

# Run only StatsBomb data ingestion
ingestify run --config config.yaml --provider statsbomb

# Test run without saving data
ingestify run --config config.yaml --dry-run

# Run with a specific bucket
ingestify run --config config.yaml --bucket production

# Run only a specific source
ingestify run --config config.yaml --source statsbomb_match

# Run with debug logging
ingestify run --config config.yaml --debug

# Run without event handlers
ingestify run --config config.yaml --disable-events
```

### list

List datasets in the dataset store.

```bash
ingestify list [OPTIONS]
```

#### Options

- `--config FILE`: Path to the configuration file
- `--bucket BUCKET`: Storage bucket to use
- `--count`: Only show the count of datasets
- `--debug`: Enable debug logging

#### Examples

```bash
# List all datasets
ingestify list --config config.yaml

# List datasets in a specific bucket
ingestify list --config config.yaml --bucket production

# Show only the count of datasets
ingestify list --config config.yaml --count
```

### delete

Delete a dataset from the dataset store.

```bash
ingestify delete [OPTIONS] DATASET_ID
```

#### Arguments

- `DATASET_ID`: Identifier of the dataset to delete
  - Can be a full dataset ID: `3f7d8e9a-1234-5678-90ab-cdef01234567`
  - Or a selector expression: `provider=statsbomb/dataset_type=match/competition_id=11/season_id=90/match_id=3788741`

#### Options

- `--config FILE`: Path to the configuration file
- `--bucket BUCKET`: Storage bucket to use
- `--debug`: Enable debug logging

#### Examples

```bash
# Delete a dataset by ID
ingestify delete --config config.yaml 3f7d8e9a-1234-5678-90ab-cdef01234567

# Delete a dataset by selector
ingestify delete --config config.yaml provider=statsbomb/dataset_type=match/competition_id=11/season_id=90/match_id=3788741

# Delete from a specific bucket
ingestify delete --config config.yaml --bucket production 3f7d8e9a-1234-5678-90ab-cdef01234567
```

### init

Initialize a new project from a template (currently disabled).

```bash
ingestify init [OPTIONS] PROJECT_NAME
```

#### Arguments

- `PROJECT_NAME`: Name of the project to create

#### Options

- `--template TEMPLATE`: Template to use (choices: "wyscout", "statsbomb_github")

#### Examples

```bash
# Initialize a new project with Statsbomb template
ingestify init --template statsbomb_github my_project

# Initialize a new project with Wyscout template
ingestify init --template wyscout my_project
```

Note: This command is currently disabled. See [GitHub issue #11](https://github.com/PySport/ingestify/issues/11).

## Environment Variables

Ingestify recognizes the following environment variables:

- `INGESTIFY_CONFIG_FILE`: Default path to the configuration file
- Other environment variables can be referenced in the configuration file using the `!ENV` tag

## Logging

Ingestify logs to stderr with the following format:

```
YYYY-MM-DD HH:MM:SS [LEVEL] module_name: Message
```

- Default log level is INFO
- Debug logging can be enabled with the `--debug` flag

## Exit Codes

- `0`: Command completed successfully
- `1`: Command failed due to a configuration error or other issue