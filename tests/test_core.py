import shutil
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from sklearn.datasets import make_regression

from timelake import TimeLake
from timelake.constants import DATASETS_FOLDER
from timelake.models import DatasetEntry

PATH = Path("./timelake_tests")


# Fixture to handle cleanup after all tests
@pytest.fixture(scope="session", autouse=True)
def cleanup_timelake_path():
    if PATH.exists():
        shutil.rmtree(PATH)

    # Yielding to the test
    yield

    if PATH.exists():
        shutil.rmtree(PATH)


def create_sample_data():
    n_hours = 24 * 30
    n_features = 10
    X, _ = make_regression(n_samples=n_hours, n_features=n_features)
    timestamps = [datetime(2023, 1, 1) + timedelta(hours=i) for i in range(n_hours)]

    return pl.DataFrame(
        X, schema=[f"feature_{i}" for i in range(n_features)]
    ).with_columns(pl.Series("date", timestamps))


def test_create_timelake():
    """Test creating a TimeLake without initial data"""
    lake = TimeLake.create(
        path=PATH,
        timestamp_column="date",
    )
    assert lake.path == str(PATH)
    assert not (PATH / DATASETS_FOLDER).exists()  # No datasets folder created yet


def test_create_dataset():
    """Test explicit dataset creation"""
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    dataset_name = "test_dataset"
    lake.create_dataset(name=dataset_name, df=df)

    # Verify dataset was created in correct location
    dataset_path = PATH / DATASETS_FOLDER / dataset_name
    assert dataset_path.exists()

    # Verify dataset is readable
    dataset = lake.get_dataset(dataset_name)
    assert dataset is not None
    assert dataset.name == dataset_name


def test_write_to_new_dataset():
    """Test writing to a new dataset creates it automatically"""
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    dataset_name = "write_dataset"
    lake.write(df=df, name=dataset_name)

    # Verify dataset was created
    dataset_path = PATH / DATASETS_FOLDER / dataset_name
    assert dataset_path.exists()

    # Verify data was written
    dataset = lake.get_dataset(dataset_name)
    assert dataset is not None


def test_upsert_to_new_dataset():
    """Test upserting to a new dataset creates it automatically"""
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    dataset_name = "upsert_dataset"
    lake.upsert(df=df, name=dataset_name)

    # Verify dataset was created
    dataset_path = PATH / DATASETS_FOLDER / dataset_name
    assert dataset_path.exists()

    # Verify data was written
    dataset = lake.get_dataset(dataset_name)
    assert dataset is not None


def test_write_requires_dataset_name():
    """Test that write operation requires a dataset name"""
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    with pytest.raises(TypeError):
        lake.write(df=df)  # Missing name parameter


def test_dataset_in_correct_location():
    """Test that datasets are created in the _timelake_datasets folder"""
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    dataset_name = "location_test"
    lake.create_dataset(name=dataset_name, df=df)

    # Verify correct path structure
    expected_path = PATH / DATASETS_FOLDER / dataset_name
    dataset = lake.get_dataset(dataset_name)
    assert Path(dataset.path) == expected_path


def test_write_timelake():
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)
    lake.write(df, name="test_write")

    # Verify data was written
    dataset = lake.get_dataset("test_write")
    assert dataset is not None


def test_read_timelake():
    lake = TimeLake.open(path=PATH)
    df = lake.read(dataset="test_write")
    assert df is not None
    assert df.shape[1] == 13  # 10 features + 1 inserted_at, date, date_day column


def test_read_with_date_range():
    lake = TimeLake.open(path=PATH)
    df = lake.read(dataset="test_write", start_date="2023-01-01", end_date="2023-01-02")
    # We get the latest inserted_at date since we inserted twice from the previous tests (meaning that we have 96 rows)
    df = df.filter(pl.col("inserted_at") == df["inserted_at"].max())
    assert df.shape[0] == 48  # 48 hours in the range, end_date is inclusive
    assert df["date"].min() >= datetime(2023, 1, 1, 0, 0, 0)
    assert df["date"].max() <= datetime(2023, 1, 2, 23, 59, 59)


def test_duplicate_inserts():
    tests_insert_count = 0  # Note: Check why this is reset, we already inserted twice in the previous tests
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)
    lake.write(df, name="test_duplicate_inserts")
    lake.write(df, name="test_duplicate_inserts")  # Duplicate insert
    # Verify data was written
    dataset = lake.get_dataset("test_duplicate_inserts")
    assert dataset is not None
    tests_insert_count += 2  # we inserted two more times in this test
    df = lake.read(dataset="test_write")
    assert df.shape[0] == int(tests_insert_count) * len(create_sample_data())


def test_upsert_insert_new_rows():
    """
    Test that the upsert method inserts new rows when no matching timestamp exists.
    """
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    # Create a new DataFrame with non-overlapping timestamps
    new_data = pl.DataFrame(
        {
            "date": [datetime(2023, 2, 1), datetime(2023, 2, 2)],
            "feature_0": [1.0, 2.0],
            "feature_1": [3.0, 4.0],
            "feature_2": [5.0, 6.0],
            "feature_3": [7.0, 8.0],
            "feature_4": [9.0, 10.0],
            "feature_5": [11.0, 12.0],
            "feature_6": [13.0, 14.0],
            "feature_7": [15.0, 16.0],
            "feature_8": [17.0, 18.0],
            "feature_9": [19.0, 20.0],
        }
    )

    # Perform upsert
    lake.upsert(new_data, name="test_write")

    # Read data and verify new rows are inserted
    df = lake.read(dataset="test_write")
    assert df.filter(pl.col("date") == datetime(2023, 2, 1)).shape[0] == 1
    assert df.filter(pl.col("date") == datetime(2023, 2, 2)).shape[0] == 1


def test_upsert_update_existing_rows():
    """
    Test that the upsert method updates existing rows when matching timestamps exist.
    """
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    # Create a new DataFrame with overlapping timestamps
    updated_data = pl.DataFrame(
        {
            "date": [datetime(2023, 1, 1, 0, 0, 0), datetime(2023, 1, 1, 1, 0, 0)],
            "feature_0": [999.0, 888.0],  # Updated values
            "feature_1": [777.0, 666.0],
            "feature_2": [555.0, 444.0],
            "feature_3": [333.0, 222.0],
            "feature_4": [111.0, 101.0],
            "feature_5": [91.0, 81.0],
            "feature_6": [71.0, 61.0],
            "feature_7": [51.0, 41.0],
            "feature_8": [31.0, 21.0],
            "feature_9": [11.0, 1.0],
        }
    )

    # Perform upsert
    lake.upsert(updated_data, name="test_write")

    # Read data and verify rows are updated
    df = lake.read(dataset="test_write")
    updated_row = df.filter(pl.col("date") == datetime(2023, 1, 1, 0, 0, 0))
    assert updated_row["feature_0"][0] == 999.0
    assert updated_row["feature_1"][0] == 777.0


def test_upsert_mixed_insert_and_update():
    """
    Test that the upsert method handles both inserting new rows and updating existing rows.
    """
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    # Create a new DataFrame with both overlapping and non-overlapping timestamps
    mixed_data = pl.DataFrame(
        {
            "date": [
                datetime(2023, 1, 1, 0, 0, 0),  # Existing row (to be updated)
                datetime(2023, 2, 1, 0, 0, 0),  # New row (to be inserted)
            ],
            "feature_0": [555.0, 123.0],
            "feature_1": [444.0, 456.0],
            "feature_2": [333.0, 789.0],
            "feature_3": [222.0, 101.0],
            "feature_4": [111.0, 202.0],
            "feature_5": [91.0, 303.0],
            "feature_6": [71.0, 404.0],
            "feature_7": [51.0, 505.0],
            "feature_8": [31.0, 606.0],
            "feature_9": [11.0, 707.0],
        }
    )

    # Perform upsert
    lake.upsert(mixed_data, name="test_write")

    # Read data and verify both update and insert
    df = lake.read(dataset="test_write")

    # Verify updated row
    updated_row = df.filter(pl.col("date") == datetime(2023, 1, 1, 0, 0, 0))
    assert updated_row["feature_0"][0] == 555.0
    assert updated_row["feature_1"][0] == 444.0

    # Verify inserted row
    inserted_row = df.filter(pl.col("date") == datetime(2023, 2, 1, 0, 0, 0))
    assert inserted_row.shape[0] == 1
    assert inserted_row["feature_0"][0] == 123.0
    assert inserted_row["feature_1"][0] == 456.0


def test_get_dataset():
    """Test getting dataset by name"""
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    dataset_name = "test_dataset"
    lake.create_dataset(name=dataset_name, df=df)

    # Get dataset by name
    dataset = lake.get_dataset(dataset_name)
    assert dataset is not None
    assert dataset.name == dataset_name
    assert isinstance(dataset, DatasetEntry)

    # Test non-existent dataset
    assert lake.get_dataset("non_existent") is None


def test_create_dataset_with_partitions():
    """Test that datasets are created with partition columns from the TimeLake config"""
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)

    dataset_name = "partitioned_dataset"
    lake.create_dataset(name=dataset_name, df=df)

    # Verify dataset was created with correct partition columns
    dataset = lake.get_dataset(dataset_name)
    assert dataset is not None
    assert dataset.partition_columns == lake.config.partition_by
