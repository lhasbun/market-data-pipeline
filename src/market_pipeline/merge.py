import pandas as pd

class MergeConflictError(Exception):
    pass

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

def merge_existing(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """
    Merge existing stored data with newly ingested data
    Exact duplicates are dropped, conflicting duplicates raise an error
    """
    
    merged = pd.concat([existing, new], ignore_index=True) # Concatenate dataframes

    dupes = merged[merged.duplicated(subset=["timestamp"], keep=False)] # Detect duplicate timestamps

    if not dupes.empty:
        if not dupes.duplicated().all(): # Check if duplicates are exact
            raise MergeConflictError( 
                f"Conflicting duplicate timestamps detected:\n{dupes}" # If duplicates raise error
            )
        merged = merged.drop_duplicates(subset=["timestamp"], keep="first") # Drop exact duplicates

    merged["year"] = merged["timestamp"].dt.year.astype(int) # Add partition columns 
    merged["month"] = merged["timestamp"].dt.month.astype(int)

    merged = merged.sort_values(["timestamp"]).reset_index(drop=True)

    return merged