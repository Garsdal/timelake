import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import polars as pl
from deltalake import DeltaTable, write_deltalake

from timelake.base import (
    BaseTimeLake,
    BaseTimeLakePreprocessor,
    BaseTimeLakeStorage,
)
from timelake.preprocessor import TimeLakePreprocessor
from timelake.storage import TimeLakeStorage


class TimeLake(BaseTimeLake):
    def __init__(
        self,
        path: str,
        timestamp_column: str,
        metadata: Dict[str, Any],
        storage: Optional[BaseTimeLakeStorage] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ):
        self.path = path
        self.timestamp_column = timestamp_column
        self.metadata = metadata
        self.storage = storage or TimeLakeStorage(path)
        self.preprocessor = preprocessor or TimeLakePreprocessor()

        self.storage.ensure_directories()

    @classmethod
    def create(
        cls,
        path: str,
        df: pl.DataFrame,
        timestamp_column: str,
        partition_by: Optional[List[str]] = None,
        storage: Optional[BaseTimeLakeStorage] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        storage = storage or TimeLakeStorage(path)
        preprocessor = preprocessor or TimeLakePreprocessor()

        storage.ensure_directories()

        partition_by = (
            [timestamp_column, *partition_by] if partition_by else [timestamp_column]
        )
        metadata = {
            "timelake_version": "0.1.0",
            "created_at": datetime.now().isoformat(),
            "timestamp_column": timestamp_column,
            "inserted_at_column": "_inserted_at",
            "partition_by": partition_by,
            "lake_id": str(uuid.uuid4()),
        }

        prepped_df = preprocessor.add_inserted_at_column(df)
        write_deltalake(path, prepped_df, partition_by=partition_by)
        storage.save_metadata(metadata)

        return cls(path, timestamp_column, metadata, storage, preprocessor)

    @classmethod
    def open(
        cls,
        path: str,
        storage: Optional[BaseTimeLakeStorage] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        storage = storage or TimeLakeStorage(path)
        metadata = storage.load_metadata()
        return cls(path, metadata["timestamp_column"], metadata, storage, preprocessor)

    def write(
        self,
        df: pl.DataFrame,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        if self.timestamp_column not in df.columns:
            raise ValueError(
                f"Timestamp column '{self.timestamp_column}' not found in DataFrame"
            )

        df_ts = self.preprocessor.add_inserted_at_column(df)
        df_ts.write_delta(
            self.path,
            mode=mode,
            delta_write_options={"partition_by": self.metadata["partition_by"]},
        )

    def read(self) -> pl.DataFrame:
        dt = DeltaTable(self.path)
        return pl.read_delta(dt)
