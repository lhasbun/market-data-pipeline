import pandas as pd

def merge_data(existing: pd.DataFrame | None, new: pd.DataFrame) -> pd.DataFrame:
    """
    Merges existing and new DataFrames.
    Ensures no duplicates and correct ordering.

    :param existing: Existing Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type existing: DataFrame or None
    :param new: Existing Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type new: DataFrame 
    :return: Merged Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :rtype: DataFrame
    """

    if existing is None: # If no existing no merge needed, ponly return new
        return new

    df = pd.concat(objs=[existing, new], ignore_index=True) # Merge the existing and new DataFrames

    df = df.drop_duplicates(subset=["timestamp"]) # Drop duplicate timestamps

    df = df.sort_values("timestamp").reset_index(drop=True) # Sort ascending

    return df