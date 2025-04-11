import os
import yaml

# Define project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Directories
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
REPORTS_DIR = os.path.join(PROJECT_ROOT, 'Reports')


def load_environments():
    config_path = os.path.join(os.path.dirname(__file__), "environment.yaml")
    config_path = os.path.abspath(config_path)  # Convert to absolute path

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    env = config.get("environment")
    if env not in config.get("environments", {}):
        raise KeyError(
            f"Environment '{env}' not found in config.yaml. Available environments: {list(config.get('environments', {}).keys())}")

    return config["environments"][env]


environment = load_environments()
