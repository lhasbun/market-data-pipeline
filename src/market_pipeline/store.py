import shutil
import os
from typing import Tuple
import pandas as pd 
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from .schema import enforce_schema
from .load import load_existing

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

    df = df.copy()

    df["year"] = df["timestamp"].dt.year.astype(str) # Add partition columns as str
    df["month"] = df["timestamp"].dt.month.astype(str).str.zfill(2)

    df.convert_dtypes() 

    path = Path(data_dir) / f"symbol={symbol}" # Build base directory E.g. data/ symbol=AAPL/

    df.to_parquet(
        path=path,
        partition_cols=["year", "month"],
        engine="pyarrow",
        index=False
    )

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

    existing = load_existing(symbol=symbol)
    

    data_dir = config.get("data_dir", "data/")
    store(symbol=symbol, df=df, data_dir=data_dir)

def detect_changed_partitions(df: pd.DataFrame):
    """
    Return list of [year, month] tuples stores in merged DataFrame

    :param df: Merged OHLCV data DataFrame
    :type df: DataFrame
    """

    return (
        df[["year", "month"]].drop_duplicates()
        .apply(lambda row: (int(row["year"]), int(row["month"])), axis=1)
        .tolist()
    )

def delete_partition(base_path: str, symbol: str, year: str, month: str):
    """
    Delete old partition directories

    :param base_path: Base data directory path
    :type base_path: str
    :param symbol: Stock ticker symbol subdirectory
    :type symbol: str
    :param year: Year subdirectory
    :type year: str
    :param month: Month subdirectory
    :type month: str
    """

    # Directory to be deleted
    path = os.path.join(base_path, f"symbol={symbol}", f"year={year}", f"month={month}")

    if os.path.exists(path):
        shutil.rmtree(path)

def write_changed_partitions(df: pd.DataFrame, base_path: str, symbol: str, changed: Tuple[str, str]):
    """
    Write only changed partitions to Parquet

    :param df: Pandas DataFrame 
    :type df: DataFrame
    :param base_path: Base data directory path
    :type base_path: str
    :param symbol: Stock ticker symbol subdirectory
    :type symbol: str
    :param changed: Changed Month and Year subdirectories
    :type changed: Tuple[str, str]
    """

    for year, month in changed:
        part_df = df[(df["year"] == year) & (df["month"] == month)]

        delete_partition(base_path=base_path, symbol=symbol, year=year, month=month)

        part_df.to_parquet(
            path=base_path,
            partition_cols=["year","month"],
            engine="pyarrow",
            index=False
        )

        print(f"[store] Rewrote partition {symbol} {year}-{month:02d}")

