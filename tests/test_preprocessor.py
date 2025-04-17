from datetime import datetime

import polars as pl
import pytest

from timelake.constants import TimeLakeColumns
from timelake.preprocessor import TimeLakePreprocessor


@pytest.fixture
def sample_df():
    return pl.DataFrame(
        {
            "date": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "asset_id": ["AAPL", "MSFT"],
            "price": [150, 300],
        }
    )


def test_validate_dataframe_success(sample_df):
    preprocessor = TimeLakePreprocessor()
    # Should not raise any exceptions
    preprocessor.validate_dataframe(sample_df, "date")


def test_validate_dataframe_empty_df():
    preprocessor = TimeLakePreprocessor()
    df = pl.DataFrame(schema={"date": pl.Datetime, "asset_id": pl.String})
    with pytest.raises(ValueError, match="DataFrame is empty."):
        preprocessor.validate_dataframe(df, "date")


def test_validate_dataframe_missing_timestamp(sample_df):
    preprocessor = TimeLakePreprocessor()
    with pytest.raises(ValueError, match="Timestamp column 'missing_ts' is missing."):
        preprocessor.validate_dataframe(sample_df, "missing_ts")


def test_validate_partitions_success(sample_df):
    preprocessor = TimeLakePreprocessor()
    # Should not raise any exceptions
    preprocessor.validate_partitions(sample_df, ["date", "asset_id"])


def test_validate_partitions_missing_column(sample_df):
    preprocessor = TimeLakePreprocessor()
    with pytest.raises(ValueError, match="Partition column 'missing_col' is missing."):
        preprocessor.validate_partitions(sample_df, ["date", "missing_col"])


def test_validate_partitions_empty():
    preprocessor = TimeLakePreprocessor()
    with pytest.raises(ValueError, match="Partition columns are empty."):
        preprocessor.validate_partitions(pl.DataFrame(), [])


def test_validate_partitions_duplicate_columns(sample_df):
    preprocessor = TimeLakePreprocessor()
    with pytest.raises(ValueError, match="Partition columns must be unique."):
        preprocessor.validate_partitions(sample_df, ["date", "date"])


def test_run_success(sample_df):
    preprocessor = TimeLakePreprocessor()
    processed_df = preprocessor.run(sample_df, "date")
    assert "date_day" in processed_df.columns  # Default partition column
    assert (
        TimeLakeColumns.INSERTED_AT.value in processed_df.columns
    )  # Inserted at column


def test_run_invalid_dataframe():
    preprocessor = TimeLakePreprocessor()
    df = pl.DataFrame(schema={"date": pl.Datetime, "asset_id": pl.String})
    with pytest.raises(ValueError, match="DataFrame is empty."):
        preprocessor.run(df, "date")


def test_run_missing_timestamp(sample_df):
    preprocessor = TimeLakePreprocessor()
    with pytest.raises(ValueError, match="Timestamp column 'missing_ts' is missing."):
        preprocessor.run(sample_df, "missing_ts")
