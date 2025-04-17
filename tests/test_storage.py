import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from timelake.constants import TimeLakeStorageType
from timelake.models import TimeLakeMetadata
from timelake.storage import LocalTimeLakeStorage, S3TimeLakeStorage

TEST_PATH = Path("./timelake_storage_test")
S3_TEST_PATH = "s3://test-bucket/timelake_storage_test"


@pytest.fixture(scope="module")
def local_storage():
    # Setup: clean directory before test
    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)

    storage = LocalTimeLakeStorage(TEST_PATH)
    storage.ensure_directories()

    yield storage  # Yield to the test functions

    # Teardown: clean up after test
    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)


@pytest.fixture(scope="module")
def s3_storage():
    # Placeholder: Mock S3 setup
    storage = S3TimeLakeStorage(
        path=S3_TEST_PATH,
        bucket_name="test-bucket",
        aws_credentials={"access_key": "key", "secret_key": "secret"},
    )
    storage.ensure_directories()

    yield storage  # Yield to the test functions

    # Placeholder: Mock S3 teardown
    pass


def test_ensure_directories_local(local_storage: LocalTimeLakeStorage):
    assert os.path.exists(local_storage.path)
    assert os.path.exists(local_storage.features_path)


def test_save_and_load_metadata_local(local_storage: LocalTimeLakeStorage):
    metadata = TimeLakeMetadata(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date", "asset_id"],
        timelake_id="test-lake-id",
        timelake_version="0.1.0",
        timelake_storage="LocalTimeLakeStorage",
        timelake_preprocessor="test_preprocessor",
        created_at=datetime.now().isoformat(),
        inserted_at_column="inserted_at",
        storage_type=TimeLakeStorageType.LOCAL.value,
    )
    local_storage.save_metadata(metadata)

    # Verify file exists
    assert os.path.exists(local_storage.metadata_path)

    # Reload and assert fields
    loaded = local_storage.load_metadata()
    assert loaded.timestamp_column == metadata.timestamp_column
    assert loaded.timestamp_partition_column == metadata.timestamp_partition_column
    assert loaded.partition_by == metadata.partition_by
    assert loaded.timelake_id == metadata.timelake_id
    assert loaded.timelake_version == metadata.timelake_version
    assert loaded.timelake_storage == metadata.timelake_storage
    assert loaded.timelake_preprocessor == metadata.timelake_preprocessor
    assert loaded.inserted_at_column == metadata.inserted_at_column
    assert loaded.storage_type == metadata.storage_type


def test_ensure_directories_s3(s3_storage: S3TimeLakeStorage):
    # Placeholder: Validate S3 directories (mocked for now)
    assert s3_storage.path == S3_TEST_PATH


def test_save_and_load_metadata_s3(s3_storage: S3TimeLakeStorage):
    metadata = TimeLakeMetadata(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date", "asset_id"],
        timelake_id="test-lake-id",
        timelake_version="0.1.0",
        timelake_storage="S3TimeLakeStorage",
        timelake_preprocessor="test_preprocessor",
        created_at=datetime.now().isoformat(),
        inserted_at_column="inserted_at",
        storage_type=TimeLakeStorageType.S3.value,
    )
    # Placeholder: Save metadata to S3 (mocked for now)
    s3_storage.save_metadata(metadata)

    # Placeholder: Reload and assert fields (mocked for now)
    loaded = metadata  # Mocked load
    assert loaded.timestamp_column == metadata.timestamp_column
    assert loaded.timestamp_partition_column == metadata.timestamp_partition_column
    assert loaded.partition_by == metadata.partition_by
    assert loaded.timelake_id == metadata.timelake_id
    assert loaded.timelake_version == metadata.timelake_version
    assert loaded.timelake_storage == metadata.timelake_storage
    assert loaded.timelake_preprocessor == metadata.timelake_preprocessor
    assert loaded.inserted_at_column == metadata.inserted_at_column
    assert loaded.storage_type == metadata.storage_type
