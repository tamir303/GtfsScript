import argparse

import yaml

# Default config file
DEFAULT_CONFIG_FILE = "config/config.yaml"

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Process GTFS data and insert into database.")
parser.add_argument("--config", help="Path to the config file", default=DEFAULT_CONFIG_FILE)
args = parser.parse_args()


class Config:
    CONFIG_KEYS = ["database", "user", "password", "host", "port"]

    def __init__(self, filename):
        with open(filename, 'r') as f:
            self.config = yaml.safe_load(f)

            # Check if all keys are present in the config file
            for key in self.CONFIG_KEYS:
                if key not in self.config:
                    raise ValueError(f"Configuration file is missing key: {key}")

    def get_database(self):
        return self.config["database"]

    def get_user(self):
        return self.config["user"]

    def get_password(self):
        return self.config["password"]

    def get_host(self):
        return self.config["host"]

    def get_port(self):
        return self.config["port"]


config = Config(args.config)
