import pandas as pd

# Canonical column order for the pipeline
EXPECTED_COLUMNS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce the canonical OHLCV schema:
    - Select only expected columns
    - Convert timestamp to UTC-aware datetime
    - Ensure correct ordering
    """
    df = df.copy()

    # Ensure all required columns exist
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Reorder and subset
    df = df[EXPECTED_COLUMNS]

    # Normalize timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Sort timestamps 
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df

