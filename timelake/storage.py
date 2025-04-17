import json
import os
import uuid
from typing import List

from timelake.base import BaseTimeLakePreprocessor, BaseTimeLakeStorage
from timelake.models import TimeLakeMetadata


class TimeLakeStorage(BaseTimeLakeStorage):
    def __init__(self, path: str):
        self.path = path
        self.features_path = os.path.join(path, "_timelake_features")
        self.metadata_path = os.path.join(path, "_timelake_metadata.json")

    def ensure_directories(self):
        os.makedirs(self.path, exist_ok=True)
        os.makedirs(self.features_path, exist_ok=True)

    def create_metadata(
        self,
        timestamp_column: str,
        partition_by: List[str],
        preprocessor: BaseTimeLakePreprocessor,
    ) -> TimeLakeMetadata:
        timestamp_partition_column = preprocessor.get_timestamp_partition_column(
            timestamp_column
        )
        return TimeLakeMetadata(
            timestamp_column=timestamp_column,
            timestamp_partition_column=timestamp_partition_column,
            partition_by=partition_by,
            timelake_id=str(uuid.uuid4()),
            timelake_storage=self.__class__.__name__,
            timelake_preprocessor=preprocessor.__class__.__name__,
        )

    def save_metadata(self, metadata: TimeLakeMetadata):
        with open(self.metadata_path, "w") as f:
            json.dump(metadata.model_dump(), f, indent=2)

    def load_metadata(self) -> TimeLakeMetadata:
        if not os.path.exists(self.metadata_path):
            raise FileNotFoundError(f"No metadata found at {self.metadata_path}")
        with open(self.metadata_path, "r") as f:
            data = json.load(f)
            return TimeLakeMetadata(**data)
