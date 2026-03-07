from collections import defaultdict
from typing import Dict, Tuple

_metrics : Dict[Tuple[str, str], int] = defaultdict(int)

def record(provider: str, event: str) -> None:
    """
    Record a metric event for a provider.

    :param provider: OHLCV data provider
    :type provider: str
    :param event: Success or Failure
    :type event: str
    """
    _metrics[(provider, event)] += 1

def dump_metrics() -> Dict[Tuple[str, str], int]:
    """
    Return copy of all collected metrics.

    :return: Dictionary with recorded metrics
    :rtype: Dict[Tuple[str, str], int]
    """
    return dict(_metrics)

def reset_metrics() -> None:
    """
    Reset all metrics.
    """
    _metrics.clear()