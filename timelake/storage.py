import os
from pathlib import Path

from timelake.base import BaseTimeLakeStorage
from timelake.constants import StorageType


class TimeLakeStorage(BaseTimeLakeStorage):
    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def create_storage(
        storage_type: StorageType, path: Path, **kwargs
    ) -> "TimeLakeStorage":
        if storage_type == StorageType.LOCAL:
            return LocalTimeLakeStorage(path)
        elif storage_type == StorageType.S3:
            return S3TimeLakeStorage(path, **kwargs)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")


class LocalTimeLakeStorage(TimeLakeStorage):
    def __init__(self, path: Path):
        super().__init__(path)

    def ensure_directories(self):
        os.makedirs(self.path, exist_ok=True)

    def get_storage_options(self) -> dict:
        return {}


class S3TimeLakeStorage(TimeLakeStorage):
    def __init__(self, path: str):
        super().__init__(path)
        self.bucket_name = self._extract_bucket_name(path)
        self._validate_aws_credentials()

    @staticmethod
    def _extract_bucket_name(path: str) -> str:
        """
        Big note here: When we treat S3 paths using Path() we convert "//" to "/"
        """
        if not path.startswith("s3://"):
            raise ValueError(f"Invalid S3 path: {path}. Must start with 's3:/'.")
        bucket_name = path.split("/")[2]  # Extract bucket name from the path
        if not bucket_name:
            raise ValueError(f"Bucket name could not be derived from path: {path}")
        return bucket_name

    @staticmethod
    def _validate_aws_credentials():
        required_env_vars = ["AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(
                f"Missing required AWS environment variables: {', '.join(missing_vars)}"
            )

    def ensure_directories(self):
        # Placeholder: Ensure S3 bucket and paths exist
        pass

    def get_storage_options(self) -> dict:
        return {
            "AWS_REGION": os.getenv("AWS_REGION"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        }
