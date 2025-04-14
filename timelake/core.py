import uuid
from typing import List, Literal, Optional

import polars as pl
from deltalake import DeltaTable, write_deltalake

from timelake.base import (
    BaseTimeLake,
    BaseTimeLakePreprocessor,
    BaseTimeLakeStorage,
)
from timelake.models import TimeLakeMetadata
from timelake.preprocessor import TimeLakePreprocessor
from timelake.storage import TimeLakeStorage


class TimeLake(BaseTimeLake):
    def __init__(
        self,
        timestamp_column: str,
        metadata: TimeLakeMetadata,
        storage: Optional[BaseTimeLakeStorage] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ):
        self.timestamp_column = timestamp_column
        self.metadata = metadata
        self.storage = storage
        self.preprocessor = preprocessor
        self.path = self.storage.path

        self.storage.ensure_directories()

    @classmethod
    def create(
        cls,
        path: str,
        df: pl.DataFrame,
        timestamp_column: str,
        partition_by: List[str] = [],
        storage: BaseTimeLakeStorage = None,
        preprocessor: BaseTimeLakePreprocessor = None,
    ) -> "TimeLake":
        storage = storage or TimeLakeStorage(path)
        preprocessor = preprocessor or TimeLakePreprocessor()

        storage.ensure_directories()
        preprocessor.validate(df, timestamp_column)

        partition_by = preprocessor.resolve_partitions(
            df, timestamp_column, partition_by
        )
        df = preprocessor.enrich_partitions(df, timestamp_column)
        df = preprocessor.add_inserted_at_column(df)

        metadata = TimeLakeMetadata(
            timestamp_column=timestamp_column,
            partition_by=partition_by,
            timelake_id=str(uuid.uuid4()),
            timelake_storage=storage.__class__.__name__,
            timelake_preprocessor=preprocessor.__class__.__name__,
        )

        write_deltalake(path, df, partition_by=partition_by)
        storage.save_metadata(metadata)

        return cls(timestamp_column, metadata, storage, preprocessor)

    @classmethod
    def open(
        cls,
        path: str,
        storage: Optional[BaseTimeLakeStorage] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        storage = storage or TimeLakeStorage(path)
        metadata = storage.load_metadata()
        preprocessor = preprocessor or TimeLakePreprocessor()
        return cls(metadata.timestamp_column, metadata, storage, preprocessor)

    def write(
        self,
        df: pl.DataFrame,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        self.preprocessor.validate(df, self.timestamp_column)
        df = self.preprocessor.enrich_partitions(df, self.timestamp_column)

        self.preprocessor.validate_partitions(df, self.metadata.partition_by)
        df = self.preprocessor.add_inserted_at_column(df)

        df.write_delta(
            self.path,
            mode=mode,
            delta_write_options={"partition_by": self.metadata.partition_by},
        )

    def read(self) -> pl.DataFrame:
        dt = DeltaTable(self.path)
        return pl.read_delta(dt)
