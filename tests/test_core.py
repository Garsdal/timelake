import shutil
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest
from sklearn.datasets import make_regression

from timelake import TimeLake

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
    df = create_sample_data()
    lake = TimeLake.create(
        path=PATH,
        df=df,
        timestamp_column="date",
    )
    assert lake.path == PATH


def test_write_timelake():
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)
    lake.write(df)


def test_read_timelake():
    lake = TimeLake.open(path=PATH)
    df = lake.read()
    assert df.shape[1] == 13  # 10 features + 1 inserted_at, date, date_day column


def test_read_with_date_range():
    lake = TimeLake.open(path=PATH)
    df = lake.read(start_date="2023-01-01", end_date="2023-01-02")
    # We get the latest inserted_at date since we inserted twice from the previous tests (meaning that we have 96 rows)
    df = df.filter(pl.col("inserted_at") == df["inserted_at"].max())
    assert df.shape[0] == 48  # 48 hours in the range, end_date is inclusive
    assert df["date"].min() >= datetime(2023, 1, 1, 0, 0, 0)
    assert df["date"].max() <= datetime(2023, 1, 2, 23, 59, 59)


def test_duplicate_inserts():
    tests_insert_count = 2  # we already inserted twice in the previous tests
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)
    lake.write(df)
    lake.write(df)
    tests_insert_count += 2  # we inserted two more times in this test
    df = lake.read()
    assert df.shape[0] == tests_insert_count * len(create_sample_data())
