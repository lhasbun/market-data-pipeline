import time
import random
from typing import Callable, TypeVar

T = TypeVar("T") # Make retry return generic

def retry(operation: Callable[[], T], retries=3, base_delay=1.0, jitter=0.3) -> T:
    """
    Retry wrapper
    """
    for attempt in range(1, retries + 1):
        try:
            return operation()
        except Exception as e:
            if attempt == retries:
                raise

            delay = base_delay * (2 ** (attempt - 1)) # Exponential backoff 
            delay += random.uniform(0, jitter) # Jitter to avoid synchronized retries

            print(f"[retry] Attempt {attempt} failed: {e}. Retrying in {delay:.2f}s...")
            time.sleep(delay)

    return operation()

