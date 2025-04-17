import os
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from timelake.models import TimeLakeMetadata
from timelake.storage import TimeLakeStorage

TEST_PATH = Path("./timelake_storage_test")


@pytest.fixture(scope="module")
def storage():
    # Setup: clean directory before test
    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)

    storage = TimeLakeStorage(TEST_PATH)
    storage.ensure_directories()

    yield storage  # Yield to the test functions

    # Teardown: clean up after test
    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)


def test_ensure_directories(storage: TimeLakeStorage):
    assert os.path.exists(storage.path)
    assert os.path.exists(storage.features_path)


def test_save_and_load_metadata(storage: TimeLakeStorage):
    metadata = TimeLakeMetadata(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date", "asset_id"],
        timelake_id="test-lake-id",
        timelake_version="0.1.0",
        timelake_storage="test_storage",
        timelake_preprocessor="test_preprocessor",
        created_at=datetime.now().isoformat(),
        inserted_at_column="inserted_at",
    )
    storage.save_metadata(metadata)

    # Verify file exists
    assert os.path.exists(storage.metadata_path)

    # Reload and assert fields
    loaded = storage.load_metadata()
    assert loaded.timestamp_column == metadata.timestamp_column
    assert loaded.timestamp_partition_column == metadata.timestamp_partition_column
    assert loaded.partition_by == metadata.partition_by
    assert loaded.timelake_id == metadata.timelake_id
    assert loaded.timelake_version == metadata.timelake_version
    assert loaded.timelake_storage == metadata.timelake_storage
    assert loaded.timelake_preprocessor == metadata.timelake_preprocessor
    assert loaded.inserted_at_column == metadata.inserted_at_column
