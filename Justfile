# List available commands.
@default:
    just --list

# Install the locked development environment.
[group('development')]
@sync:
    uv sync --locked --group dev

# Run Ruff checks, format code, and check types.
[group('checks')]
@lint:
    uv run ruff check
    uv run ruff format
    uv run mypy

# Run all tests (requires the brokers from docker/docker-compose.yaml).
[group('tests')]
@test:
    uv run pytest

# Run unit tests without live MQTT brokers.
[group('tests')]
@test-unit:
    uv run pytest --ignore=tests/test_brokers

# Build documentation with warnings treated as errors.
[group('docs')]
@docs:
    uv run mkdocs build --strict

# Build source and wheel distributions.
[group('packaging')]
@build:
    uv build
