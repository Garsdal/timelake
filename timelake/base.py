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
    @abstractmethod
    def ensure_directories(self) -> None: ...

    @abstractmethod
    def save_metadata(self, metadata: TimeLakeMetadata) -> None: ...

    @abstractmethod
    def load_metadata(self) -> TimeLakeMetadata: ...


class BaseTimeLakePreprocessor(ABC):
    @abstractmethod
    def validate(self, df: pl.DataFrame, timestamp_column: str) -> None: ...

    @abstractmethod
    def resolve_partitions(
        self, df: pl.DataFrame, timestamp_column: str, user_partitions: List[str]
    ) -> List[str]: ...

    @abstractmethod
    def enrich_partitions(
        self, df: pl.DataFrame, timestamp_column: str
    ) -> pl.DataFrame: ...

    @abstractmethod
    def add_inserted_at_column(self, df: pl.DataFrame) -> pl.DataFrame: ...
