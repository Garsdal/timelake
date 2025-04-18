import os
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from deltalake import DeltaTable

from timelake.catalog import TimeLakeCatalog
from timelake.models import DatasetEntry, TimeLakeEntry

TEST_PATH = Path("./timelake_catalog_test")


@pytest.fixture(scope="function")
def catalog():
    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)

    os.makedirs(TEST_PATH)

    catalog = TimeLakeCatalog.create_catalog(str(TEST_PATH))

    yield catalog

    if TEST_PATH.exists():
        shutil.rmtree(TEST_PATH)


def test_create_catalog(catalog: TimeLakeCatalog):
    # Just check that catalog was created successfully
    assert os.path.exists(f"{TEST_PATH}/_timelake_catalog")


def test_add_entry(catalog: TimeLakeCatalog):
    config = TimeLakeEntry(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date_day"],
        timelake_id="test-id",
        timelake_storage="LocalTimeLakeStorage",
        timelake_preprocessor="TimeLakePreprocessor",
        name="Test TimeLake",
    )

    entry_id = catalog.add_entry(config)
    assert entry_id is not None

    # Verify entry was added
    entries = catalog.list_entries()
    assert len(entries) == 1
    assert isinstance(entries[0], TimeLakeEntry)
    assert entries[0].timestamp_column == "date"


def test_get_entry(catalog: TimeLakeCatalog):
    config = TimeLakeEntry(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date_day"],
        timelake_id="test-id",
        timelake_storage="LocalTimeLakeStorage",
        timelake_preprocessor="TimeLakePreprocessor",
        name="Test TimeLake",
    )

    entry_id = catalog.add_entry(config)

    # Get the entry
    entry = catalog.get_entry(entry_id)
    assert entry is not None
    assert isinstance(entry, TimeLakeEntry)
    assert entry.timestamp_column == "date"
    assert entry.name == "Test TimeLake"

    # Try getting non-existent entry
    assert catalog.get_entry("non-existent") is None


def test_update_entry(catalog: TimeLakeCatalog):
    config = TimeLakeEntry(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date_day"],
        timelake_id="test-id",
        timelake_storage="LocalTimeLakeStorage",
        timelake_preprocessor="TimeLakePreprocessor",
        name="Test TimeLake",
    )

    entry_id = catalog.add_entry(config)

    # Update the entry
    assert catalog.update_entry(entry_id, {"name": "Updated TimeLake"})

    # Verify the update
    entry = catalog.get_entry(entry_id)
    assert entry.name == "Updated TimeLake"

    # Try updating non-existent entry
    assert not catalog.update_entry("non-existent", {"name": "Something"})


def test_delete_entry(catalog: TimeLakeCatalog):
    config = TimeLakeEntry(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date_day"],
        timelake_id="test-id",
        timelake_storage="LocalTimeLakeStorage",
        timelake_preprocessor="TimeLakePreprocessor",
        name="Test TimeLake",
    )

    entry_id = catalog.add_entry(config)

    # Delete the entry
    assert catalog.delete_entry(entry_id)

    # Verify it's gone
    assert catalog.get_entry(entry_id) is None

    # Try deleting non-existent entry
    assert not catalog.delete_entry("non-existent")


def test_list_entries_by_type(catalog: TimeLakeCatalog):
    # Add a timelake config
    timelake_config = TimeLakeEntry(
        timestamp_column="date",
        timestamp_partition_column="date_day",
        partition_by=["date_day"],
        timelake_id="test-id-1",
        timelake_storage="LocalTimeLakeStorage",
        timelake_preprocessor="TimeLakePreprocessor",
        name="Test TimeLake",
    )
    catalog.add_entry(timelake_config)

    # Add a dataset config
    dataset_config = DatasetEntry(
        name="test_dataset",
        path="/path/to/dataset",
        dataset_schema={"date": "datetime", "value": "float"},
        partition_columns=["date_day"],
    )
    catalog.add_entry(dataset_config)

    # List all entries
    all_entries = catalog.list_entries()
    assert len(all_entries) == 2
    assert isinstance(all_entries[0], TimeLakeEntry)
    assert isinstance(all_entries[1], DatasetEntry)

    # List only timelake configs
    timelake_entries = catalog.list_entries(entry_type="timelake_config")
    assert len(timelake_entries) == 1
    assert isinstance(timelake_entries[0], TimeLakeEntry)

    # List only datasets
    dataset_entries = catalog.list_entries(entry_type="dataset")
    assert len(dataset_entries) == 1
    assert isinstance(dataset_entries[0], DatasetEntry)
    assert dataset_entries[0].name == "test_dataset"


def test_create_dataset(catalog: TimeLakeCatalog):
    df = pl.DataFrame(
        {
            "date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "value": [100.0, 200.0],
        }
    )

    dataset_entry = catalog.create_dataset(
        name="test_dataset",
        path=Path(f"{TEST_PATH}/test_dataset"),
        df=df,
        partition_columns=["date"],
    )

    # Verify dataset entry
    assert isinstance(dataset_entry, DatasetEntry)
    assert dataset_entry.name == "test_dataset"
    assert dataset_entry.path == f"{TEST_PATH}/test_dataset"
    assert dataset_entry.partition_columns == ["date"]

    # Verify dataset was written to the path
    dt = DeltaTable(dataset_entry.path)
    written_df = pl.read_delta(dt)
    assert written_df.shape == df.shape
    assert sorted(written_df.columns) == sorted(df.columns)
