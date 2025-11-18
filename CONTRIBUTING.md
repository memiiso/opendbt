# Contributing to OpenDBT

## Development Setup

```bash
git clone https://github.com/memiiso/opendbt.git
cd opendbt
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[test]"
```

## Running Tests Locally

### Unit Tests

```bash
python -m pytest tests/
```

### Airflow Plugin Tests

**Prerequisites:**
- Docker installed and running
- `testcontainers` package installed

**Run tests:**

```bash
# Enable Airflow tests (they're skipped by default)
export RUN_AIRFLOW_TESTS=1

# Run all Airflow tests
python -m unittest discover -s tests -p "test_airflow.py" -v

# Or run specific test classes
python -m unittest tests.test_airflow.TestAirflowLegacyMode -v
python -m unittest tests.test_airflow.TestAirflowMultiProjectMode -v
```

## Running CI Tests Locally with Act

[Act](https://github.com/nektos/act) allows you to run GitHub Actions workflows locally using Docker.

**Install Act:**

```bash
# macOS
brew install act

# Linux
curl -s https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash

# Windows (with Chocolatey)
choco install act-cli
```

**Run CI tests locally:**

```bash
# Run all tests (same as CI)
act push

# Run specific job
act push -j 'test-dbt-version'

# Run with specific Python version (matrix)
act push -j 'test-dbt-version' --matrix python-version:3.12

# Run with verbose output
act push -j 'test-dbt-version' -v
```

**Environment variables for Act:**

```bash
# Set plugin mode for Airflow tests
AIRFLOW_PLUGIN_MODE=multi act push -j 'test-dbt-version'
```

**Useful Act commands:**

```bash
# List available workflows
act -l

# Run specific workflow
act -W .github/workflows/tests-dbt-version.yml

# Use different Docker image (faster)
act --pull=false

# Run without pulling Docker images
act --pull=false push
```

## Code Style

We use `pylint` for code quality:

```bash
python -m pylint opendbt/
```

## Submitting Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes
4. Run tests locally (including Act if modifying CI)
5. Commit with descriptive messages
6. Push and create a Pull Request

## Testing Checklist

Before submitting a PR:

- [ ] Unit tests pass: `pytest tests/`
- [ ] Airflow tests pass (if applicable): `RUN_AIRFLOW_TESTS=1 python -m unittest`
- [ ] Pylint passes: `pylint opendbt/`
- [ ] CI tests pass locally: `act push -j 'test-dbt-version'`
- [ ] Documentation updated (if applicable)
