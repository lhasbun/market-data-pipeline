# Market Data ETL Pipeline

A production-style Python pipeline for ingesting, validating, and storing daily OHLCV market data from multiple data providers. Built for reliability, reproducibility, and extensibility.

## Features
- Multi-source OHLCV ingestion (Yahoo Finance, Alpha Vantage, etc.)
- Unified schema + data normalization
- Data validation (missing timestamps, duplicates, ordering)
- Parquet-based data lake with partitioning
- Config-driven architecture (YAML/TOML)
- CLI for running ingestion jobs
- Dockerized runtime environment
- Optional scheduling (cron, Airflow, or Prefect)
- Unit + integration tests
- DuckDB analytics examples

## Project Structure
market-data-pipeline/
  src/market_pipeline/
  tests/
  data/
  pyproject.toml
  README.md
  .gitignore

## Installation
Run: pip install . or pip install -e .

## Running the Pipeline
market-pipeline update --symbol AAPL
market-pipeline update

## Configuration
Config lives in src/market_pipeline/config/default_config.yaml

## Data Storage Layout
Partitioned Parquet lake under data/

## Development
pytest, ruff, black

## Docker
Build and run the image

## Example Analysis (DuckDB)
Simple SQL query example

## Roadmap
Future enhancements listed

## License
MIT License

## Author
Luis Hasbun