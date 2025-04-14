from datetime import datetime
from typing import List

import polars as pl

from timelake.base import BaseTimeLakePreprocessor


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
        self, df: pl.DataFrame, timestamp_column: str, user_partitions: List[str]
    ) -> List[str]:
        return (
            [timestamp_column] + user_partitions
            if user_partitions
            else [timestamp_column]
        )

    def enrich_partitions(
        self, df: pl.DataFrame, timestamp_column: str
    ) -> pl.DataFrame:
        # TO-DO: Add logic here to enrich partitions if needed, e.g. day, month, year
        return df

    def add_inserted_at_column(self, df: pl.DataFrame) -> pl.DataFrame:
        now = datetime.now().isoformat()
        return df.with_columns(pl.lit(now).alias("_inserted_at"))
