from typing import Literal, Optional

import polars as pl
from deltalake import DeltaTable, write_deltalake

from timelake.base import (
    BaseTimeLake,
    BaseTimeLakePreprocessor,
    BaseTimeLakeStorage,
)
from timelake.constants import TimeLakeColumns, TimeLakeStorageType
from timelake.models import TimeLakeMetadata
from timelake.preprocessor import TimeLakePreprocessor
from timelake.storage import TimeLakeStorage


class TimeLake(BaseTimeLake):
    def __init__(
        self,
        timestamp_column: str,
        storage: BaseTimeLakeStorage,
        preprocessor: BaseTimeLakePreprocessor,
        metadata: Optional[TimeLakeMetadata] = None,
    ):
        self.timestamp_column = timestamp_column
        self.storage = storage
        self.preprocessor = preprocessor
        self.path = self.storage.path

        # Load metadata or create it if not provided
        self.metadata = metadata or self.storage.create_metadata(
            timestamp_column=self.timestamp_column,
            preprocessor=self.preprocessor,
        )

        self.storage.ensure_directories()

    @classmethod
    def create(
        cls,
        path: str,
        df: pl.DataFrame,
        timestamp_column: str,
        storage_type: TimeLakeStorageType = TimeLakeStorageType.LOCAL,
        storage_kwargs: Optional[dict] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        storage_kwargs = storage_kwargs or {}
        storage = TimeLakeStorage.create_storage(storage_type, path, **storage_kwargs)
        storage.ensure_directories()

        preprocessor = preprocessor or TimeLakePreprocessor()
        df = preprocessor.run(df, timestamp_column)

        instance = cls(
            timestamp_column=timestamp_column,
            storage=storage,
            preprocessor=preprocessor,
        )

        partition_by = preprocessor.get_default_partitions(timestamp_column)
        write_deltalake(path, df, partition_by=partition_by)
        storage.save_metadata(instance.metadata)

        return instance

    @classmethod
    def open(
        cls,
        path: str,
        storage_type: TimeLakeStorageType = TimeLakeStorageType.LOCAL,
        storage_kwargs: Optional[dict] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        storage_kwargs = storage_kwargs or {}
        storage = TimeLakeStorage.create_storage(storage_type, path, **storage_kwargs)
        metadata = storage.load_metadata()
        preprocessor = preprocessor or TimeLakePreprocessor()
        return cls(
            timestamp_column=metadata.timestamp_column,
            storage=storage,
            preprocessor=preprocessor,
            metadata=metadata,
        )

    def write(
        self,
        df: pl.DataFrame,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        df = self.preprocessor.run(df, self.timestamp_column)
        df.write_delta(
            self.path,
            mode=mode,
            delta_write_options={"partition_by": self.metadata.partition_by},
        )

    def upsert(self, df: pl.DataFrame) -> None:
        df = self.preprocessor.run(df, self.timestamp_column)
        df.write_delta(
            self.path,
            mode="merge",
            delta_merge_options={
                "predicate": f"s.{self.timestamp_column} = t.{self.timestamp_column}",
                "source_alias": "s",
                "target_alias": "t",
            },
        ).when_matched_update_all().when_not_matched_insert_all().execute()

    def read(
        self,
        signal: str = None,
        start_date: str = None,
        end_date: str = None,
    ) -> pl.DataFrame:
        dt = DeltaTable(self.path)

        filters = []
        if signal:
            filters.append((TimeLakeColumns.SIGNAL.value, "=", signal))

        timestamp_partition_column = self.metadata.timestamp_partition_column
        if start_date:
            filters.append((timestamp_partition_column, ">=", start_date))
        if end_date:
            filters.append((timestamp_partition_column, "<=", end_date))

        # Important: Use pyarrow options to push down partition filters
        if filters:
            return pl.read_delta(
                dt,
                pyarrow_options={"partitions": filters},
                use_pyarrow=True,
            )
        return pl.read_delta(dt)
