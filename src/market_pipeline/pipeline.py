from .config_loader import load_config
from .ingest import fetch
from .validate import validate
from .store import store_with_config
from .load import load_existing
from .merge import merge_data
from .retry import retry
import pandas as pd

def run_pipeline(symbol: str | None = None):
    """
    Unified pipeline entry point.
    - Loads config
    - Determines which symbols to ingest
    - Loads existing Parquet data (if any)
    - Fetches new data from providers
    - Merges existing + new data
    - Validates merged dataset
    - Stores back to Parquet (partitioned)

    :param symbol: Stock ticker symbol
    :type symbol: str
    """

    config = load_config() # Load config dict

    symbols = [symbol] if symbol else config["symbols"] # If no symbol(s) provided use config

    for sym in symbols:
        
        existing = load_existing(symbol=sym, data_dir=config["data_dir"]) # Load existing data (if exists)

        if existing is not None:
            print(f"Loaded {len(existing)} existing rows for {sym}")
        else:
            print(f"No existing data found for {sym}")

        new = fetch_with_retry(symbol=sym, config=config) # Fetch new data
        print(f"Fetched {len(new)} new rows for {sym}")

        merged = merge_data(existing=existing, new=new) # Merge data
        print(f"Merged dataset has {len(merged)} rows")

        validate(merged) # Validate merged data
        print("Validation passed")

        store_with_config(symbol=sym, df=merged, config=config) # Store merged dataset
        print(f"Stored updated data for {sym}")

def fetch_with_retry(symbol: str, config: dict):
    return retry(lambda: fetch(symbol=symbol, config=config))

