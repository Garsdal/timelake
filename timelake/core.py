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
from timelake.constants import (
    DATASETS_FOLDER,
    CatalogEntryType,
    StorageType,
)
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
        timestamp_column: str,
        storage_type: StorageType = StorageType.LOCAL,
        storage_kwargs: Optional[dict] = None,
        preprocessor: Optional[BaseTimeLakePreprocessor] = None,
    ) -> "TimeLake":
        """
        Create a new TimeLake.

        Args:
            path: Path to store the TimeLake
            timestamp_column: Name of timestamp column
            storage_type: Type of storage to use
            storage_kwargs: Storage-specific kwargs
            preprocessor: Optional preprocessor to use

        Returns:
            TimeLake: The created TimeLake instance
        """
        # Initialize all core components
        path = ensure_path(path)
        storage_kwargs = storage_kwargs or {}
        storage = TimeLakeStorage.create_storage(storage_type, path, **storage_kwargs)
        preprocessor = preprocessor or TimeLakePreprocessor()
        catalog = TimeLakeCatalog.create_catalog(path, storage.get_storage_options())

        # Ensure storage is ready
        storage.ensure_directories()

        # Create and return TimeLake instance
        return cls(
            timestamp_column=timestamp_column,
            storage=storage,
            preprocessor=preprocessor,
            catalog=catalog,
        )

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
        name: str,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        """
        Write data to a dataset in the TimeLake.

        Args:
            df: Data to write
            name: Name of the dataset
            mode: Write mode (append or overwrite)
        """
        processed_df = self.preprocessor.run(df, self.timestamp_column)
        dataset = self.get_or_create_dataset(name=name, df=processed_df)

        processed_df.write_delta(
            dataset.path,
            mode=mode,
            delta_write_options={"partition_by": self.config.partition_by},
            storage_options=self.storage.get_storage_options(),
        )

    def upsert(self, df: pl.DataFrame, name: str) -> None:
        """
        Upsert data into a dataset in the TimeLake.

        Args:
            df: Data to upsert
            name: Name of the dataset
        """
        processed_df = self.preprocessor.run(df, self.timestamp_column)
        dataset = self.get_or_create_dataset(name=name, df=processed_df)

        processed_df.write_delta(
            dataset.path,
            mode="merge",
            delta_merge_options={
                "predicate": f"s.{self.timestamp_column} = t.{self.timestamp_column}",
                "source_alias": "s",
                "target_alias": "t",
            },
        ).when_matched_update_all().when_not_matched_insert_all().execute()

    def read(
        self,
        dataset: str,
        start_date: str = None,
        end_date: str = None,
    ) -> pl.DataFrame:
        """
        Read data from a specific dataset in the TimeLake.

        Args:
            dataset: Name of the dataset to read from
            start_date: Start date filter
            end_date: End date filter

        Returns:
            pl.DataFrame: The filtered data
        """
        # Get the dataset entry
        dataset_entry = self.get_dataset(name=dataset)
        if not dataset_entry:
            raise ValueError(f"Dataset '{dataset}' does not exist in the TimeLake.")

        # Get the dataset path
        dataset_path = dataset_entry.path

        # Initialize the DeltaTable
        dt = DeltaTable(
            dataset_path, storage_options=self.storage.get_storage_options()
        )

        # Build filters
        filters = []
        timestamp_partition_column = self.config.timestamp_partition_column
        if start_date:
            filters.append((timestamp_partition_column, ">=", start_date))
        if end_date:
            filters.append((timestamp_partition_column, "<=", end_date))

        # Read the data with or without filters
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
    ) -> str:
        """
        Create a new dataset in the TimeLake.

        Args:
            name: Name of the dataset
            df: Data to write

        Returns:
            str: ID of the created dataset
        """
        dataset_path = Path(self.path) / DATASETS_FOLDER / name

        # Process the data
        processed_df = self.preprocessor.run(df, self.timestamp_column)

        # Delegate dataset creation to the catalog
        return self.catalog.create_dataset(
            name=name,
            path=str(dataset_path),
            df=processed_df,
            partition_columns=self.config.partition_by,
            storage_options=self.storage.get_storage_options(),
        )

    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all datasets in the TimeLake.

        Returns:
            List[Dict[str, Any]]: List of dataset configs
        """
        return self.catalog.list_entries(entry_type=CatalogEntryType.DATASET.value)

    def get_dataset(self, name: str) -> Optional[DatasetEntry]:
        """
        Get a dataset by name.

        Args:
            name: Name of the dataset

        Returns:
            Optional[DatasetEntry]: Dataset entry if found
        """
        return self.catalog.get_entry_by_name(
            name=name, entry_type=CatalogEntryType.DATASET.value
        )

    def get_or_create_dataset(
        self,
        name: str,
        df: Optional[pl.DataFrame] = None,
    ) -> DatasetEntry:
        """
        Get an existing dataset or create a new one.

        Args:
            name: Name of the dataset
            df: Optional data to write if creating new dataset

        Returns:
            DatasetEntry: The dataset entry
        """
        # Try to get existing dataset
        existing = self.catalog.get_entry_by_name(
            name=name, entry_type=CatalogEntryType.DATASET.value
        )
        if existing:
            return existing

        # Create new dataset if not found
        if df is None:
            raise ValueError("DataFrame must be provided when creating new dataset")

        return self.create_dataset(name=name, df=df)
