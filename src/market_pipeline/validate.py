import pandas as pd 

class ValidationError(Exception):
    """
    Raised when data validation fails
    """
    pass

def check_missing_timestamps(df: pd.DataFrame):
    """
    Check for missing timestamps in table

    :param df: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type df: DataFrame
    """

    if df["timestamp"].isna().any():
        raise ValidationError("Missing timestamps detected.")
    
def check_duplicates(df: pd.DataFrame):
    """
    Check for duplicate timestamps in table

    :param df: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type df: DataFrame
    """

    if df["timestamp"].duplicated().any():
        raise ValidationError("Duplicate timestamp(s) detected.")
    
def check_sorted(df: pd.DataFrame):
    """
    Check timestamps are sorted ASC order

    :param df: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type df: DataFrame
    """

    if not df["timestamp"].is_monotonic_increasing:
        raise ValidationError("Timestamps are not sorted in ascending order.")
    
def check_negative_values(df: pd.DataFrame):
    """
    Check for negative values in numeric columns 

    :param df: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type df: DataFrame
    """

    numeric_cols = ["open", "high", "low", "close", "volume"] # OHLCV columns

    for col in numeric_cols:
        if(df[col] < 0).any():
            raise ValidationError(f"Negative value(s) detected in column: {col}")

def validate(df: pd.DataFrame):
    """
    Runs all validation checks
    Raises ValidationError if any check fails

    :param df: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :type df: DataFrame
    """

    check_missing_timestamps(df)
    check_duplicates(df)
    check_sorted(df)
    check_negative_values(df)

    return True