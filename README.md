# Market Data ETL Pipeline

A production‑style Python pipeline for ingesting, validating, and storing daily OHLCV market data from multiple data providers. Designed for reliability, reproducibility, and extensibility — following real data engineering patterns.

---

## Features

- Multi‑source OHLCV ingestion  
  - Yahoo Finance (full history, free)
  - Alpha Vantage (free‑tier fallback)
- Unified schema + data normalization
- Config‑driven architecture (YAML + `.env`)
- Data validation (schema enforcement, timestamp normalization)
- Partitioned Parquet data lake (`symbol=…/year=…/month=…`)
- Unified ingestion interface with provider priority + fallback
- CLI for running ingestion jobs (Typer)
- Dockerized runtime environment
- Unit + integration tests
- DuckDB analytics examples

---

## Project Structure

market-data-pipeline/
	src/
		market_pipeline/
			config/
				default_config.yaml
				schema.yaml
			ingest.py
			schema.py	
			config_loader.py
	data/	# Parquet data lake (gitignored)                 
	tests/
	.venv/	# Virtual environment (gitignored)
	.env # API keys + secrets (gitignored)
	pyproject.toml
	README.md
	.gitignore

---

## Installation & Environment Setup

### 1. Create and activate a virtual environment
python -m venv .venv ..venv\Scripts\activate
### 2. Install the project
pip install -e .
### 3. Add your API keys to `.env`
ALPHA_VANTAGE_API_KEY=your_key_here

---

## Configuration

All non‑secret configuration lives in:
src/market_pipeline/config/default_config.yaml
Secrets (API keys) stay in .env.

## Schema Definition
Canonical OHLCV schema lives in:
src/market_pipeline/config/schema.yaml

ohlcv_schema:
  timestamp: datetime64[ns, UTC]
  open: float
  high: float
  low: float
  close: float
  volume: int

Schema enforcement is implemented in:
src/market_pipeline/schema.py

## Ingestion Layer
Yahoo Finance (primary provider)
- Full historical OHLCV
- Free
- Reliable
Alpha Vantage (fallback provider)
- Free tier returns last ~100 days (outputsize=compact)
- Used only when Yahoo fails

Unified ingestion interface

fetch("AAPL")

Automatically:
- Tries Yahoo
- Falls back to Alpha Vantage
- Returns normalized OHLCV data

## Running the Pipeline
Fetch data for a single symbol:

market-pipeline update --symbol AAPL

Fetch all symbols from config:

market-pipeline update

## Data Storage Layout

Data is stored as partitioned Parquet under data/:

data/
  symbol=AAPL/
    year=2024/
      month=01/
        data.parquet

This structure is optimized for DuckDB, Spark, and analytical workloads.

## License
MIT License

## Author
Luis Eduardo Hasbun







