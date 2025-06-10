# Contributing to Ingestify

We welcome contributions to Ingestify! This guide explains how to contribute to the project.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork to your local machine
3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

## Development Workflow

### Setting Up a Development Environment

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install the package in development mode:
   ```bash
   pip install -e ".[dev]"
   ```

3. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

### Running Tests

Run the test suite using pytest:

```bash
pytest
```

Or run a specific test:

```bash
pytest tests/test_engine.py
```

### Code Style

We follow the [Black](https://black.readthedocs.io/) code style. The pre-commit hooks will automatically format your code when you commit.

## Project Structure

```
ingestify/
├── application/        # Application layer (use cases)
│   ├── dataset_store.py
│   ├── ingestion_engine.py
│   └── ...
├── domain/             # Domain layer (core business logic)
│   ├── models/         # Domain models
│   │   ├── dataset/    # Dataset-related models
│   │   ├── event/      # Event system
│   │   ├── ingestion/  # Ingestion models
│   │   └── ...
│   └── services/       # Domain services
├── infra/              # Infrastructure layer
│   ├── fetch/          # Data fetching implementations
│   ├── serialization/  # Serialization utilities
│   ├── sink/           # Data sink implementations
│   ├── source/         # Data source implementations
│   └── store/          # Storage implementations
├── cmdline.py          # Command-line interface
└── main.py             # Main entry point
```

## Adding a New Source

To add a new data source:

1. Create a new class in `infra/source/` that extends `Source`
2. Implement the required methods:
   - `discover_selectors`: Return available data selectors
   - `find_datasets`: Fetch and return datasets for given selectors

Example:

```python
from datetime import datetime
from ingestify import DatasetResource, Source
from ingestify.domain.models.dataset.dataset import DatasetState

class NewSource(Source):
    def __init__(self, name, api_key, base_url="https://api.example.com"):
        super().__init__(name=name)
        self.api_key = api_key
        self.base_url = base_url
        self.provider = "new_provider"
    
    def discover_selectors(self, dataset_type: str):
        # Implement discovery logic
        return [
            {
                "competition_id": "1",
                "season_id": "2023"
            }
        ]
    
    def find_datasets(self, dataset_type, **selectors):
        # Implement dataset fetching logic
        # Create and yield DatasetResource objects
        pass
```

## Adding a New Storage Backend

To add a new storage backend:

1. For file storage, create a class that extends `FileRepository`
2. For metadata storage, create a class that extends `DatasetRepository`

## Adding a New Event Subscriber

To add a new event subscriber:

1. Create a class that extends `Subscriber`
2. Implement the event handlers you need:
   - `on_dataset_created`
   - `on_revision_added`

## Pull Request Guidelines

1. Ensure your code passes all tests
2. Add tests for new functionality
3. Update documentation if needed
4. Follow the code style guidelines
5. Keep pull requests focused on a single topic
6. Write a clear description of the changes

## Documentation

When updating documentation:

1. Update docstrings for public APIs
2. Update the documentation files in `/docs`
3. Add examples for new features

## Releasing

1. Update version in `__init__.py`
2. Update CHANGELOG.md
3. Create a GitHub release
4. Publish to PyPI:
   ```bash
   python -m build
   twine upload dist/*
   ```

## Code of Conduct

Please be respectful and inclusive when contributing to the project. We aim to maintain a welcoming community for all contributors.

## License

By contributing, you agree that your contributions will be licensed under the project's license.