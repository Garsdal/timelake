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


def test_validate_success(sample_df):
    preprocessor = TimeLakePreprocessor()
    preprocessor.validate(sample_df, "date")  # Should not raise


def test_validate_empty_df():
    preprocessor = TimeLakePreprocessor()
    df = pl.DataFrame(schema={"date": pl.Datetime, "asset_id": pl.String})
    with pytest.raises(ValueError, match="DataFrame is empty"):
        preprocessor.validate(df, "date")


def test_validate_missing_timestamp(sample_df):
    preprocessor = TimeLakePreprocessor()
    with pytest.raises(ValueError, match="Timestamp column 'missing_ts' is missing"):
        preprocessor.validate(sample_df, "missing_ts")


def test_resolve_partitions_with_user_partitions():
    preprocessor = TimeLakePreprocessor()
    result = preprocessor.resolve_partitions("date", ["asset_id"])
    assert result == ["date_day", "asset_id"]


def test_resolve_partitions_without_user_partitions():
    preprocessor = TimeLakePreprocessor()
    result = preprocessor.resolve_partitions("date", [])
    assert result == ["date_day"]


def test_add_inserted_at_column(sample_df):
    preprocessor = TimeLakePreprocessor()
    enriched_df = preprocessor.add_inserted_at_column(sample_df)
    inserted_at_column = TimeLakeColumns.INSERTED_AT.value

    assert inserted_at_column in enriched_df.columns
    assert enriched_df[inserted_at_column].n_unique() == 1
