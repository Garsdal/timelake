from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal, Optional

import polars as pl


class BaseTimeLake(ABC):
    path: str
    timestamp_column: str
    metadata: Dict[str, Any]

    @classmethod
    @abstractmethod
    def create(
        cls,
        path: str,
        df: pl.DataFrame,
        timestamp_column: str,
        partition_by: Optional[List[str]] = None,
    ) -> "BaseTimeLake":
        pass

    @classmethod
    @abstractmethod
    def open(cls, path: str) -> "BaseTimeLake":
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
    def save_metadata(self, metadata: Dict) -> None: ...

    @abstractmethod
    def load_metadata(self) -> Dict: ...


class BaseTimeLakePreprocessor(ABC):
    @abstractmethod
    def add_inserted_at_column(self, df: pl.DataFrame) -> pl.DataFrame: ...
