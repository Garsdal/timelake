import json
import os
from typing import Dict

from timelake.base import BaseTimeLakeStorage


class TimeLakeStorage(BaseTimeLakeStorage):
    def __init__(self, path: str):
        self.path = path
        self.features_path = os.path.join(path, "_timelake_features")
        self.metadata_path = os.path.join(path, "_timelake_metadata.json")

    def ensure_directories(self):
        os.makedirs(self.path, exist_ok=True)
        os.makedirs(self.features_path, exist_ok=True)

    def save_metadata(self, metadata: Dict):
        with open(self.metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def load_metadata(self) -> Dict:
        if not os.path.exists(self.metadata_path):
            raise FileNotFoundError(f"No metadata found at {self.metadata_path}")
        with open(self.metadata_path, "r") as f:
            return json.load(f)
