import json
import os
import uuid

from timelake.base import BaseTimeLakePreprocessor, BaseTimeLakeStorage
from timelake.constants import TimeLakeStorageType
from timelake.models import TimeLakeMetadata


class TimeLakeStorage(BaseTimeLakeStorage):
    def __init__(self, path: str):
        self.path = path

    @staticmethod
    def create_storage(
        storage_type: TimeLakeStorageType, path: str, **kwargs
    ) -> "TimeLakeStorage":
        if storage_type == TimeLakeStorageType.LOCAL:
            return LocalTimeLakeStorage(path)
        elif storage_type == TimeLakeStorageType.S3:
            return S3TimeLakeStorage(path, **kwargs)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")


class LocalTimeLakeStorage(TimeLakeStorage):
    def __init__(self, path: str):
        super().__init__(path)
        self.features_path = os.path.join(path, "_timelake_features")
        self.metadata_path = os.path.join(path, "_timelake_metadata.json")

    def ensure_directories(self):
        os.makedirs(self.path, exist_ok=True)
        os.makedirs(self.features_path, exist_ok=True)

    def create_metadata(
        self,
        timestamp_column: str,
        preprocessor: BaseTimeLakePreprocessor,
    ) -> TimeLakeMetadata:
        timestamp_partition_column = preprocessor.get_timestamp_partition_column(
            timestamp_column
        )
        partition_by = preprocessor.get_default_partitions(timestamp_column)
        return TimeLakeMetadata(
            timestamp_column=timestamp_column,
            timestamp_partition_column=timestamp_partition_column,
            partition_by=partition_by,
            timelake_id=str(uuid.uuid4()),
            timelake_storage=self.__class__.__name__,
            timelake_preprocessor=preprocessor.__class__.__name__,
            storage_type=TimeLakeStorageType.LOCAL.value,
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


class S3TimeLakeStorage(TimeLakeStorage):
    def __init__(self, path: str, bucket_name: str, aws_credentials: dict):
        super().__init__(path)
        self.bucket_name = bucket_name
        self.aws_credentials = aws_credentials

    def ensure_directories(self):
        # Placeholder: Ensure S3 bucket and paths exist
        pass

    def create_metadata(
        self,
        timestamp_column: str,
        preprocessor: BaseTimeLakePreprocessor,
    ) -> TimeLakeMetadata:
        timestamp_partition_column = preprocessor.get_timestamp_partition_column(
            timestamp_column
        )
        partition_by = preprocessor.get_default_partitions(timestamp_column)
        return TimeLakeMetadata(
            timestamp_column=timestamp_column,
            timestamp_partition_column=timestamp_partition_column,
            partition_by=partition_by,
            timelake_id=str(uuid.uuid4()),
            timelake_storage=self.__class__.__name__,
            timelake_preprocessor=preprocessor.__class__.__name__,
            storage_type=TimeLakeStorageType.S3.value,
        )

    def save_metadata(self, metadata: TimeLakeMetadata):
        # Placeholder: Save metadata to S3
        pass

    def load_metadata(self) -> TimeLakeMetadata:
        # Placeholder: Load metadata from S3
        pass
