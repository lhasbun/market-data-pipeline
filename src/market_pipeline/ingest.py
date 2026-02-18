import os
import requests
import structlog
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, UTC
from .schema import enforce_schema

load_dotenv() # Load Alpha Vantage API key from .env

logger = structlog.get_logger() # Bound logger instance creation

def fetch(symbol: str, start: str | None = None, end: str | None = None, config: dict | None = None):
    """
    Unified ingestion interface.
    Tries providers in order until one succeeds.

    :param symbol: Stock ticker symbol
    :type symbol: str
    :param start: Start date YYYY-MM-DD (Default = 2015-01-01)
    :type start: date
    :param end: End date YYYY-MM-DD (Default = Today)
    :type end: date
    :param config: Configuration dictionary (default_config.yaml)
    :type config: dict
    :return: Pandas dataframe OHLCV data from Yahoo Finance, Alpha Vantage, or both for a given symbol
    :rtype: DataFrame
    """
    if config is not None:
        providers = config["providers"]["priority"] # If config dict is provided
    else:
        providers = ["yahoo", "alpha_vantage"]

    for provider in providers:
        try:
            logger.info("fetch_attempt", provider=provider, symbol=symbol) # Log fetch attempt
            # Call requested provider(s) fetch fuction(s)
            if provider == "yahoo":
                return fetch_yahoo(symbol=symbol, start=start, end=end)
            elif provider == "alpha_vantage":
                return fetch_alpha_vantage(symbol=symbol)
            raise ValueError(f"Unknown provider: {provider}") # Error chekc: if provider is not known
        except Exception as e:
            logger.warning("fetch_failed", provider=provider, symbol=symbol, error=str(e)) # Log fetch failure
    
    raise RuntimeError(f"All providers failed for symbol: {symbol}") # Error check: No result in either provider


def fetch_yahoo(symbol: str, start: str | None = "2015-01-01", end: str | None = None) -> pd.DataFrame:
    """
    Fetch daily OHLCV data from Yahoo Finance for a given symbol.
    Returns a DataFrame matching the canonical OHLCV schema.

    :param symbol: Stock ticker symbol
    :type symbol: str
    :param start: Start date YYYY-MM-DD (Default = 2015-01-01)
    :type start: date
    :param end: End date YYYY-MM-DD
    :type end: date
    :return: Pandas dataframe OHLCV data from Yahoo Finance for a given symbol
    :rtype: DataFrame
    """
    if end is None:
        end = datetime.now(tz=UTC).strftime("%Y-%m-%d") # If end is empty set to UTC now time
    
    ticker = yf.Ticker(ticker=symbol) # Initialize yf Ticker object
    df = ticker.history(start=start, end=end, interval="1d") # Create dataframe with ticker daily ticker info

    if df.empty:
        raise  ValueError(f"No data returned from Yahoo fro symbol: {symbol}") # Error check: check if dataframe is empty
    
    df = df.reset_index() # Reset index to get timestamp column

    # Rename columns to match canonical schema
    df = df.rename(
        columns={
            "Date": "timestamp",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    ) 

    df = enforce_schema(df=df) # Enforce schema

    return df

def fetch_alpha_vantage(symbol: str) -> pd.DataFrame:
    """
    Fetch daily OHLCV data from Alpha Vantage.
    Returns a DataFrame matching the canonical OHLCV schema.

    :param symbol: Stock ticker symbol
    :type symbol: str
    :return: Pandas dataframe OHLCV data from Alpha Vantage for a given symbol
    :rtype: DataFrame
    """

    api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise ValueError("Missing ALPHA_VANTAGE_API_KEY in .env") # Error check: check for missing API key in .env

    url = "https://www.alphavantage.co/query" # URL for HTTP request from Alpha Vantage
    # Parameters for Alpha Vantage API request 
    params = {
        "function": "TIME_SERIES_DAILY", # Retrieves daily OHLCV (Open, High, Low, Close, Volume) data, including split/dividend adjustments
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact", # Returns the full history (up to 20+ years)
    }

    response = requests.get(url=url, params=params)
    data = response.json() # Decode JSON response as Python object 

    if "Time Series (Daily)" not in data:
        raise ValueError(f"Unexpected Alpha Vantage response: {data}") # Error check: check if error in API response
    
    ts = data["Time Series (Daily)"]

    # Covert time series data into a DataFrame
    # Columns renamed to match schema 
    df = (
        pd.DataFrame.from_dict(data=ts, orient="index")
        .rename( 
            columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume",
            }
        )
        .reset_index()
        .rename(
            columns={
                "index": "timestamp"
            }
        )
    )

    # Convert numeric columns into appropriate type
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].astype(dtype=float)
    df["volume"] = df["volume"].astype(dtype=int)

    df = enforce_schema(df) # Enforce schema

    return df