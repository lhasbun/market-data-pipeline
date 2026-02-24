import pandas as pd
from pathlib import Path

def load_existing(symbol: str, data_dir: str = "data/") -> pd.DataFrame | None: 
    """
    Loads all existing Parquet files for ticker symbol into a DataFrame. 
    Constructs a path, checks for file existence, and returns the data or None if not found. 

    :param symbol: Stock ticker symbol
    :type symbol: str
    :param data_dir: Data directory for storage (Default = "data/")
    :type data_dir: str
    :return: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :rtype: DataFrame or None

    """
    base = Path(data_dir) / f"symbol={symbol}" # Path: data/ symbol=AAPL/

    if not base.exists(): # If directory does not exist, return None
        return None 
    
    df = pd.read_parquet(path=base, engine="pyarrow") # Load partitions for symbol 

    # Drop partition column to avoid merge conflicts

    for col in ["year", "month"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    df = df.convert_dtypes() # Reset dtypes to pure pandas types (critical!)

    return df