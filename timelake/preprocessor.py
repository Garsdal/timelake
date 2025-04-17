from datetime import datetime
from typing import List

import polars as pl

from timelake.base import BaseTimeLakePreprocessor
from timelake.constants import TimeLakeColumns


class TimeLakePreprocessor(BaseTimeLakePreprocessor):
    def validate(self, df: pl.DataFrame, timestamp_column: str) -> None:
        if df.shape[0] == 0:
            raise ValueError("DataFrame is empty.")
        if timestamp_column not in df.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' is missing.")

    def validate_partitions(self, df: pl.DataFrame, partition_by: List[str]) -> None:
        if not partition_by:
            raise ValueError("Partition columns are empty.")
        for column in partition_by:
            if column not in df.columns:
                raise ValueError(f"Partition column '{column}' is missing.")
        if len(set(partition_by)) != len(partition_by):
            raise ValueError("Partition columns must be unique.")

    def resolve_partitions(
        self, timestamp_column: str, user_partitions: List[str]
    ) -> List[str]:
        day_partition = f"{timestamp_column}_day"

        partitions = [day_partition]
        for col in user_partitions:
            if col not in partitions:
                partitions.append(col)

        return partitions

    def enrich_partitions(
        self, df: pl.DataFrame, timestamp_column: str
    ) -> pl.DataFrame:
        day_partition = f"{timestamp_column}_day"

        df = df.with_columns(
            pl.col(timestamp_column)
            .dt.truncate("1d")
            .dt.strftime("%Y-%m-%d")
            .alias(day_partition)
        )

        return df

    def add_inserted_at_column(self, df: pl.DataFrame) -> pl.DataFrame:
        now = datetime.now().isoformat()
        return df.with_columns(pl.lit(now).alias(TimeLakeColumns.INSERTED_AT.value))

    def get_timestamp_partition_column(self, timestamp_column: str) -> str:
        """Determine the timestamp partition column dynamically."""
        return f"{timestamp_column}_day"
