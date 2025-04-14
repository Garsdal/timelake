import shutil
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from timelake import TimeLake

PATH = Path("./timelake_tests")


# Fixture to handle cleanup after all tests
@pytest.fixture(scope="session", autouse=True)
def cleanup_timelake_path():
    if PATH.exists():
        shutil.rmtree(PATH)

    # Yielding to the test
    yield

    # if PATH.exists():
    #     shutil.rmtree(PATH)


def create_sample_data():
    dates = [datetime(2023, 1, 1) + timedelta(days=i) for i in range(100)]
    prices = [100 + i + (i % 7) * 3 for i in range(100)]
    volumes = [1000 + i * 10 + (i % 5) * 100 for i in range(100)]

    return pl.DataFrame(
        {
            "date": dates,
            "asset_id": ["AAPL"] * 50 + ["MSFT"] * 50,
            "price": prices,
            "volume": volumes,
        }
    )


def test_create_timelake():
    df = create_sample_data()
    lake = TimeLake.create(
        path=PATH,
        df=df,
        timestamp_column="date",
        partition_by=["asset_id"],
    )
    assert lake.path == PATH


def test_write_timelake():
    df = create_sample_data()
    lake = TimeLake.open(path=PATH)
    lake.write(df)


def test_read_timelake():
    lake = TimeLake.open(path=PATH)
    df = lake.read()
    assert df.shape == (200, 5)
    assert set(df["asset_id"].unique()) == {"AAPL", "MSFT"}
