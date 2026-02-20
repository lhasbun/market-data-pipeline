import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from .schema import enforce_schema

def store(symbol: str, df: pd.DataFrame, data_dir: str = "data/"):
    """
    Store OHLCV data in a partitioned Parquet layout:
    data/
      symbol=AAPL/
        year=2024/
          month=01/
            data.parquet

    :param symbol: Stock ticker symbol
    :type symbol: str
    :param df: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type df: DataFrame
    :param data_dir: Data directory for storage (Default = "data/")
    :type data_dir: str
    """

    df = enforce_schema(df) # Enforce schema before writing

    df["year"] = df["timestamp"].dt.year.astype(str) # Add partition columns
    df["month"] = df["timestamp"].dt.month.astype(str).str.zfill(2)

    table = pa.Table.from_pandas(df) # Convert to arrow table

    base = Path(data_dir) / f"symbol={symbol}" # Build base directory E.g. data/ symbol=AAPL/

    for year in df["year"].unique():
        for month in df["month"].unique():
            partition_path = base / f"year={year}" / f"month={month}" # E.g. year=2024/ month=01/
            partition_path.mkdir(parents=True, exist_ok=True)

            file_path = partition_path / "data.parquet"

            # Filter rows for partition
            part_df = df[(df["year"] == year) & (df["month"] == month)]
            part_table = pa.Table.from_pandas(part_df) 

            pq.write_table(part_table, file_path) # Write partition table to filepath

def store_with_config(symbol: str, df: pd.DataFrame, config: dict):
    """
    Helper function to store using config

    :param symbol: Stock ticker symbol
    :type symbol: str
    :param df: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type df: DataFrame
    :param config: Configuration file
    :type config: dict
    """
    data_dir = config.get("data_dir", "data/")
    store(symbol=symbol, df=df, data_dir=data_dir)



