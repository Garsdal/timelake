from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import polars as pl
from deltalake import DeltaTable

from timelake.base import (
    BaseTimeLake,
    BaseTimeLakeCatalog,
    BaseTimeLakePreprocessor,
    BaseTimeLakeStorage,
)
from timelake.catalog import TimeLakeCatalog
from timelake.constants import CatalogEntryType, StorageType, TimeLakeColumns
from timelake.models import DatasetEntry, TimeLakeEntry
from timelake.preprocessor import TimeLakePreprocessor
from timelake.storage import TimeLakeStorage
from timelake.utils import ensure_path


class TimeLake(BaseTimeLake):
    def __init__(
        self,
        timestamp_column: str,
        storage: BaseTimeLakeStorage,
        preprocessor: BaseTimeLakePreprocessor,
        catalog: BaseTimeLakeCatalog,
        config: Optional[TimeLakeEntry] = None,
    ):
        """
        Initialize a TimeLake instance.

        Args:
            timestamp_column: Name of the column containing timestamps
            storage: Storage backend to use
            preprocessor: Preprocessor to use
            catalog: Catalog to use
            config: Optional preloaded config to avoid redundant catalog calls
        """
        self.timestamp_column = timestamp_column
        self.storage = storage
        self.preprocessor = preprocessor
        self.catalog = catalog
        self.path = ensure_path(self.storage.path)  # Ensure path is a Path object

        # Use the provided config or fetch it from the catalog
        if config:
            self.config_id = config.id
            self.config = config
        else:
            self.config_id, self.config = self.catalog.get_or_create_timelake_config(
                timestamp_column=timestamp_column,
                preprocessor=self.preprocessor,
                storage=self.storage,
            )

        # Ensure storage is ready
        self.storage.ensure_directories()

    @classmethod
    def create(
        cls,
        path: Path | str,
        df: pl.DataFrame,
        timestamp_column: str,
        storage_type: StorageType = StorageType.LOCAL,
        storage_kwargs: Optional[dict] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        """
        Create a new TimeLake.

        Args:
            path: Path to store the TimeLake
            df: Initial data to store
            timestamp_column: Name of timestamp column
            storage_type: Type of storage to use
            storage_kwargs: Storage-specific kwargs
            preprocessor: Optional preprocessor to use

        Returns:
            TimeLake: The created TimeLake instance
        """
        path = ensure_path(path)  # Ensure path is a str
        storage_kwargs = storage_kwargs or {}
        storage = TimeLakeStorage.create_storage(storage_type, path, **storage_kwargs)
        storage.ensure_directories()
        preprocessor = preprocessor or TimeLakePreprocessor()

        # Create catalog
        catalog = TimeLakeCatalog.create_catalog(path, storage.get_storage_options())

        # Create TimeLake instance
        instance = cls(
            timestamp_column=timestamp_column,
            storage=storage,
            preprocessor=preprocessor,
            catalog=catalog,
        )

        # Process and write the initial data
        processed_df = preprocessor.run(df, timestamp_column)

        # Register the main dataset in the catalog
        catalog.create_dataset(
            name="main",
            path=path,
            df=processed_df,
            partition_columns=preprocessor.get_default_partitions(timestamp_column),
        )

        return instance

    @classmethod
    def open(
        cls,
        path: Path | str,
        storage_type: StorageType = StorageType.LOCAL,
        storage_kwargs: Optional[dict] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        """
        Open an existing TimeLake.

        Args:
            path: Path to the TimeLake
            storage_type: Type of storage
            storage_kwargs: Storage-specific kwargs
            preprocessor: Optional preprocessor to use

        Returns:
            TimeLake: The opened TimeLake instance
        """
        path = ensure_path(path)  # Ensure path is a str
        storage_kwargs = storage_kwargs or {}
        storage = TimeLakeStorage.create_storage(storage_type, path, **storage_kwargs)
        preprocessor = preprocessor or TimeLakePreprocessor()

        # Open catalog
        try:
            catalog = TimeLakeCatalog.open_catalog(path, storage.get_storage_options())
        except ValueError:
            raise ValueError(
                f"No TimeLake catalog found at {path}. Use create() for new TimeLakes."
            )

        # Fetch the timelake_config directly
        config = catalog.get_timelake_config()
        if not config:
            raise ValueError(
                f"Invalid TimeLake catalog at {path}: no timelake_config found"
            )

        # Create TimeLake instance with the preloaded config
        return cls(
            timestamp_column=config.timestamp_column,
            storage=storage,
            preprocessor=preprocessor,
            catalog=catalog,
            config=config,
        )

    def write(
        self,
        df: pl.DataFrame,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        """
        Write data to the TimeLake.

        Args:
            df: Data to write
            mode: Write mode (append or overwrite)
        """
        df = self.preprocessor.run(df, self.timestamp_column)
        df.write_delta(
            self.path,
            mode=mode,
            delta_write_options={"partition_by": self.config.partition_by},
            storage_options=self.storage.get_storage_options(),
        )

    def upsert(self, df: pl.DataFrame) -> None:
        """
        Upsert data into the TimeLake.

        Args:
            df: Data to upsert
        """
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
        """
        Read data from the TimeLake.

        Args:
            signal: Signal to filter by
            start_date: Start date filter
            end_date: End date filter

        Returns:
            pl.DataFrame: The filtered data
        """
        dt = DeltaTable(self.path, storage_options=self.storage.get_storage_options())

        filters = []
        if signal:
            filters.append((TimeLakeColumns.SIGNAL.value, "=", signal))

        timestamp_partition_column = self.config.timestamp_partition_column
        if start_date:
            filters.append((timestamp_partition_column, ">=", start_date))
        if end_date:
            filters.append((timestamp_partition_column, "<=", end_date))

        if filters:
            return pl.read_delta(
                dt,
                pyarrow_options={"partitions": filters},
                use_pyarrow=True,
            )
        return pl.read_delta(
            dt,
            use_pyarrow=True,
        )

    # Catalog-based dataset methods

    def create_dataset(
        self,
        name: str,
        df: pl.DataFrame,
        partition_columns: Optional[List[str]] = None,
        primary_key: Optional[str] = None,
    ) -> str:
        """
        Create a new dataset in the TimeLake.

        Args:
            name: Name of the dataset
            df: Data to write
            partition_columns: Columns to partition by
            primary_key: Primary key column

        Returns:
            str: ID of the created dataset
        """
        dataset_path = f"{self.path}/{name}"

        # Delegate dataset creation to the catalog
        return self.catalog.create_dataset(
            name=name,
            path=dataset_path,
            df=df,
            partition_columns=partition_columns,
            primary_key=primary_key,
            storage_options=self.storage.get_storage_options(),
        )

    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all datasets in the TimeLake.

        Returns:
            List[Dict[str, Any]]: List of dataset configs
        """
        return self.catalog.list_entries(entry_type=CatalogEntryType.DATASET.value)

    def get_dataset(self, dataset_id: str) -> Optional[DatasetEntry]:
        """
        Get a dataset by ID.

        Args:
            dataset_id: ID of the dataset

        Returns:
            Optional[DatasetEntry]: Dataset entry if found
        """
        return self.catalog.get_entry(dataset_id)
