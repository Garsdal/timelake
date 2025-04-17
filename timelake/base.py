from abc import ABC, abstractmethod
from typing import List, Literal, Optional

import polars as pl

from timelake.models import TimeLakeMetadata


class BaseTimeLake(ABC):
    path: str
    timestamp_column: str
    metadata: TimeLakeMetadata

    @classmethod
    @abstractmethod
    def create(
        cls,
        path: str,
        df: pl.DataFrame,
        timestamp_column: str,
        partition_by: Optional[List[str]] = None,
        storage: Optional["BaseTimeLakeStorage"] = None,
        preprocessor: Optional["BaseTimeLakePreprocessor"] = None,
    ) -> "BaseTimeLake":
        pass

    @classmethod
    @abstractmethod
    def open(
        cls,
        path: str,
        storage: Optional["BaseTimeLakeStorage"] = None,
        preprocessor: Optional["BaseTimeLakePreprocessor"] = None,
    ) -> "BaseTimeLake":
        pass

    @abstractmethod
    def write(
        self,
        df: pl.DataFrame,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        pass

    @abstractmethod
    def read(self) -> pl.DataFrame:
        pass


class BaseTimeLakeStorage(ABC):
    path: str

    @abstractmethod
    def ensure_directories(self) -> None: ...

    @abstractmethod
    def create_metadata(
        self,
        timestamp_column: str,
        partition_by: List[str],
        preprocessor: "BaseTimeLakePreprocessor",
    ) -> TimeLakeMetadata: ...

    @abstractmethod
    def save_metadata(self, metadata: TimeLakeMetadata) -> None: ...

    @abstractmethod
    def load_metadata(self) -> TimeLakeMetadata: ...


class BaseTimeLakePreprocessor(ABC):
    @abstractmethod
    def validate_dataframe(self, df: pl.DataFrame, timestamp_column: str) -> None: ...

    @abstractmethod
    def validate_partitions(
        self, df: pl.DataFrame, partition_by: List[str]
    ) -> None: ...

    @abstractmethod
    def get_timestamp_partition_column(self, timestamp_column: str) -> str: ...

    @abstractmethod
    def get_default_partitions(self, timestamp_column: str) -> List[str]: ...

    @abstractmethod
    def add_inserted_at_column(self, df: pl.DataFrame) -> pl.DataFrame: ...

    @abstractmethod
    def enrich_partitions(
        self, df: pl.DataFrame, timestamp_column: str
    ) -> pl.DataFrame: ...

    @abstractmethod
    def prepare_data(self, df: pl.DataFrame, timestamp_column: str) -> pl.DataFrame: ...

    @abstractmethod
    def run(
        self,
        df: pl.DataFrame,
        timestamp_column: str,
    ) -> pl.DataFrame: ...
