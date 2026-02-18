import yaml
from pathlib import Path

def load_config():
    """  
    Makes ingestion layer fully config driven

    :return: Configuration dictionary parsed from .yaml
    :rtype: dict
    """
    config_path = Path(__file__).parent/"config"/"default_config.yaml"

    with open(file=config_path, mode="r") as f:
        return yaml.safe_load(f)