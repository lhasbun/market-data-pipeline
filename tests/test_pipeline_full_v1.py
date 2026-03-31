import os
import pytest
import pandas as pd
from market_pipeline.retry import retry
from market_pipeline.metrics import record, dump_metrics, reset_metrics
from market_pipeline.store import (  
    detect_changed_partitions, 
    delete_partition,
    write_changed_partitions,
    store_with_config,
)
from market_pipeline.merge import merge_existing, MergeConflictError
from market_pipeline.validate import validate_symbol
from market_pipeline.config_loader import load_config
from pathlib import Path

#### Helpers

def make_df(rows: list):
    """
    Make a df using a list of tuples for rows of data
    e.g. rows = [
            ("2024-01-01", 100, 200, 90, 150, 1000),
        ]

    :param rows: OHLCV data in a list of Tuples
    :type rows: list(Tuple[str, float, float, float, float, float])

    """

    df = pd.DataFrame(
        data = rows, 
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df

#### Retry logic tests

def test_retry_success_after_failures():
    """
    Test retry success after failure logic
    """
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("fail")
        return 42
    
    result = retry(operation=flaky, retries=5)
    assert result == 42
    assert attempts["count"] == 3

def test_retry_exhausts_and_raises():
    """
    Test retry exhaust and runtime error raise
    """
    def always_fail():
        raise RuntimeError("ALWAYS FAIL")
    
    with pytest.raises(expected_exception=RuntimeError):
        retry(operation=always_fail, retries=2)

#### Metrics recording tests

def test_metrics_recording():
    """
    
    """
    reset_metrics()

    record(provider="alpha_vantage", event="success")
    record(provider="alpha_vantage", event="failure")
    record(provider="yfinance", event="success")

    metrics = dump_metrics()

    assert metrics[("alpha_vantage", "success")]
    assert metrics[("alpha_vantage", "failure")]
    assert metrics[("alpha_vantage", "success")]

def test_metrics_reset():
    """
    
    """
    reset_metrics()

    record(provider="alpha_vantage", event="success")
    assert dump_metrics() == {("alpha_vantage", "success"): 1}

    reset_metrics()
    assert dump_metrics() == {}

#### Merge logic tests

def test_merge_exact_duplicates():
    """
    
    """
    existing = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
    ])

    new = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
    ])

    merged = merge_existing(existing=existing, new=new)
    assert len(merged) == 1

def test_merge_conflict():
    """
    
    """
    existing = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
    ])

    new = make_df([
        ("2024-01-01", 101, 200, 90, 150, 1000),
    ])

    with pytest.raises(MergeConflictError):
        merge_existing(existing=existing, new=new)

def test_merge_non_overlapping():
    """
    
    """
    existing = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
    ])

    new = make_df([
        ("2024-01-02", 110, 210, 100, 160, 2000),
    ])

    merged = merge_existing(existing=existing, new=new)
    assert len(merged) == 2
    assert list(merged["timestamp"].dt.day) == [1, 2]

def test_merge_sorts_by_timestamp():
    """
    
    """
    existing = make_df([
        ("2024-01-02", 110, 210, 100, 160, 2000),
    ])

    new = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
    ])

    merged = merge_existing(existing=existing, new=new)
    assert list(merged["timestamp"].dt.day) == [1, 2]

#### Partition detection tests

def test_detected_changed_partitions():
    """
    
    """
    df = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
        ("2024-02-01", 110, 210, 100, 160, 2000),
    ])

    parts = detect_changed_partitions(df=df)
    assert parts == {(2024, 1), (2024, 2)}

#### Write partition test

def test_write_changed_partitions(temp_path: Path):
    """
    
    """
    config = load_config()

    base = temp_path / "data"

    df = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
    ])

    store_with_config(symbol="AAPL", df=df, config=config)

    expected_file = base / "AAPl"/ "year=2024" / "month=1" / "data.parquet"
    assert expected_file.exists()

#### Delete partition test

def test_delete_partition(tmp_path: Path):
    """
    
    """
    base = tmp_path / "data"
    part_dir = base / "AAPL" / "year=2024" / "month=1"
    part_dir.mkdir(parents=True)

    base_path = str(base)

    assert part_dir.exists()
    delete_partition(base_path=base_path, symbol="AAPL", year="2024", month="1")
    assert not part_dir.exists()

#### Store with confi test

def test_store_with_config(tmp_path: Path):
    """
    
    """
    config = load_config()

    base = tmp_path / "data"

    df = make_df([
        ("2024-01-01", 100, 200, 90, 150, 1000),
    ])

    config = {
        "data_dir": str(base),
        "symbol": "AAPL",
    }

    store_with_config(symbol="AAPL", df=df, config=config)

    expected_file = base / "AAPL" / "year=2024" / "month=1" / "data.parquet"
    assert expected_file.exists()