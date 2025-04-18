from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import polars as pl

from timelake.models import BaseCatalogEntry, TimeLakeEntry


class BaseTimeLakeCatalog(ABC):
    """Base class for TimeLake catalog implementations."""

    path: str

    @classmethod
    @abstractmethod
    def create_catalog(
        cls, path: Path, storage_options: Optional[Dict[str, Any]] = None
    ) -> "BaseTimeLakeCatalog":
        """Create a new catalog at the specified path."""
        pass

    @classmethod
    @abstractmethod
    def open_catalog(
        cls, path: Path, storage_options: Optional[Dict[str, Any]] = None
    ) -> "BaseTimeLakeCatalog":
        """Open an existing catalog at the specified path."""
        pass

    @abstractmethod
    def add_entry(self, entry: BaseCatalogEntry) -> str:
        """Add a new entry to the catalog."""
        pass

    @abstractmethod
    def get_entry(self, entry_id: str) -> Optional[Dict[str, Any]]:
        """Get an entry from the catalog by ID."""
        pass

    @abstractmethod
    def get_entry_by_name(self, name: str, entry_type: str) -> Optional[Dict[str, Any]]:
        """Get an entry from the catalog by name and type."""
        pass

    @abstractmethod
    def list_entries(self, entry_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all entries in the catalog, optionally filtered by type."""
        pass

    @abstractmethod
    def update_entry(self, entry_id: str, properties: Dict[str, Any]) -> bool:
        """Update an existing catalog entry."""
        pass

    @abstractmethod
    def delete_entry(self, entry_id: str) -> bool:
        """Delete an entry from the catalog."""
        pass

    @abstractmethod
    def create_dataset(
        self,
        name: str,
        path: Path,
        df: pl.DataFrame,
        partition_columns: Optional[List[str]] = None,
        primary_key: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create a new dataset in the catalog.
        This method is responsible for creating a dataset in the catalog
        and returning the dataset ID.
        """
        pass

    @abstractmethod
    def get_or_create_timelake_config(
        self,
        timestamp_column: str,
        preprocessor: "BaseTimeLakePreprocessor",
        storage: "BaseTimeLakeStorage",
    ) -> tuple[str, Dict[str, Any]]:
        """
        Get existing TimeLake config or create a new one if it doesn't exist.

        This method encapsulates the logic of creating a TimeLakeConfig object internally
        and handling the storage of this configuration in the catalog.

        Args:
            timestamp_column: Name of the timestamp column
            preprocessor: TimeLake preprocessor instance
            storage: TimeLake storage instance

        Returns:
            tuple[str, Dict[str, Any]]: Config ID and config dictionary
        """
        pass

    @abstractmethod
    def get_timelake_config(self) -> Optional[TimeLakeEntry]:
        """
        Get a TimeLake config by ID.

        Args:
            config_id: ID of the TimeLake config

        Returns:
            Optional[TimeLakeEntry]: TimeLake config entry
        """
        pass


class BaseTimeLake(ABC):
    path: str
    timestamp_column: str

    @classmethod
    @abstractmethod
    def create(
        cls,
        path: Path | str,
        df: pl.DataFrame,
        timestamp_column: str,
    ) -> "BaseTimeLake":
        pass

    @classmethod
    @abstractmethod
    def open(
        cls,
        path: Path | str,
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

    @staticmethod
    @abstractmethod
    def create_storage(
        storage_type: str,
        path: str,
        **kwargs,
    ) -> "BaseTimeLakeStorage":
        pass

    @abstractmethod
    def ensure_directories(self) -> None: ...

    @abstractmethod
    def get_storage_options(self) -> dict:
        """Return storage-specific options for integration with Polars."""
        pass


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
