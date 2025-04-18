import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
from deltalake import DeltaTable, write_deltalake

from timelake.base import (
    BaseTimeLakeCatalog,
    BaseTimeLakePreprocessor,
    BaseTimeLakeStorage,
)
from timelake.constants import CATALOG_TABLE_NAME, CatalogEntryType
from timelake.models import (
    BaseCatalogEntry,
    DatasetEntry,
    TimeLakeEntry,
)

# Mapping for dynamic model resolution
ENTRY_TYPE_TO_MODEL = {
    CatalogEntryType.TIMELAKE_CONFIG.value: TimeLakeEntry,
    CatalogEntryType.DATASET.value: DatasetEntry,
}


class TimeLakeCatalog(BaseTimeLakeCatalog):
    """
    Manages the catalog for a TimeLake instance. The catalog is stored as a Delta table
    and contains metadata about all objects in the TimeLake.
    """

    def __init__(self, path: str, storage_options: Optional[Dict[str, Any]] = None):
        """
        Initialize the catalog manager.

        Args:
            path: Base path of the TimeLake
            storage_options: Options for storage backend (S3, etc.)
        """
        self.path = path
        self.catalog_path = f"{path}/{CATALOG_TABLE_NAME}"
        self.storage_options = storage_options or {}

    @classmethod
    def create_catalog(
        cls, path: str, storage_options: Optional[Dict[str, Any]] = None
    ) -> "TimeLakeCatalog":
        """
        Create a new catalog at the specified path.
        """
        catalog = cls(path, storage_options)
        catalog._ensure_catalog_exists()
        return catalog

    @classmethod
    def open_catalog(
        cls, path: str, storage_options: Optional[Dict[str, Any]] = None
    ) -> "TimeLakeCatalog":
        """
        Open an existing catalog at the specified path.
        """
        catalog = cls(path, storage_options)
        if not catalog._catalog_exists():
            raise ValueError(f"No catalog found at {path}/{CATALOG_TABLE_NAME}")
        return catalog

    def _catalog_exists(self) -> bool:
        """Check if the catalog exists."""
        try:
            DeltaTable(self.catalog_path, storage_options=self.storage_options)
            return True
        except Exception:
            return False

    def _ensure_catalog_exists(self) -> None:
        """Create the catalog delta table if it doesn't exist."""
        if self._catalog_exists():
            return

        # Create an empty catalog with the required schema
        empty_df = pl.DataFrame(
            schema={
                "id": pl.String,
                "name": pl.String,
                "entry_type": pl.String,
                "created_at": pl.Datetime,
                "updated_at": pl.Datetime,
                "properties": pl.String,  # JSON serialized properties
            }
        )
        write_deltalake(
            self.catalog_path,
            empty_df,
            storage_options=self.storage_options,
        )

    def add_entry(self, entry: BaseCatalogEntry) -> str:
        """
        Add a new entry to the catalog.
        """
        # Assign a unique ID if not provided
        entry.id = entry.id or str(uuid.uuid4())

        # Prepare the entry for storage
        entry_dict = entry.model_dump()
        core_fields = {
            key: entry_dict.pop(key)
            for key in ["id", "name", "entry_type", "created_at", "updated_at"]
        }
        catalog_entry = {**core_fields, "properties": json.dumps(entry_dict)}

        # Append the new entry to the catalog
        entry_df = pl.DataFrame([catalog_entry])
        dt = DeltaTable(self.catalog_path, storage_options=self.storage_options)
        try:
            catalog_df = pl.read_delta(dt)
            catalog_df = pl.concat([catalog_df, entry_df])
        except Exception:
            catalog_df = entry_df

        # Write back to the catalog
        catalog_df.write_delta(
            self.catalog_path,
            mode="overwrite",
            storage_options=self.storage_options,
        )
        return entry.id

    def _parse_entry(self, row: Dict[str, Any]) -> BaseCatalogEntry:
        """
        Parse a catalog row into the appropriate Pydantic model.
        """
        entry_type = row["entry_type"]
        model_class = ENTRY_TYPE_TO_MODEL.get(entry_type, BaseCatalogEntry)
        properties = json.loads(row["properties"])
        entry_data = {
            "id": row["id"],
            "name": row["name"],
            "entry_type": entry_type,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            **properties,
        }
        return model_class(**entry_data)

    def get_entry(self, entry_id: str) -> Optional[BaseCatalogEntry]:
        """
        Get an entry from the catalog by ID.

        Args:
            entry_id: ID of the entry to retrieve

        Returns:
            Optional[BaseCatalogEntry]: The catalog entry if found, else None
        """
        dt = DeltaTable(self.catalog_path, storage_options=self.storage_options)
        catalog_df = pl.read_delta(dt)

        result = catalog_df.filter(pl.col("id") == entry_id)
        if len(result) == 0:
            return None

        return self._parse_entry(result.row(0, named=True))

    def get_entry_by_name(
        self, name: str, entry_type: str
    ) -> Optional[BaseCatalogEntry]:
        """Get an entry from the catalog by name and type."""
        dt = DeltaTable(self.catalog_path, storage_options=self.storage_options)
        catalog_df = pl.read_delta(dt)

        result = catalog_df.filter(
            (pl.col("name") == name) & (pl.col("entry_type") == entry_type)
        )
        if len(result) == 0:
            return None

        return self._parse_entry(result.row(0, named=True))

    def list_entries(self, entry_type: Optional[str] = None) -> List[BaseCatalogEntry]:
        """
        List catalog entries, optionally filtered by type.

        Args:
            entry_type: Filter entries by type

        Returns:
            List[BaseCatalogEntry]: List of catalog entries
        """
        dt = DeltaTable(self.catalog_path, storage_options=self.storage_options)
        catalog_df = pl.read_delta(dt)

        if entry_type:
            catalog_df = catalog_df.filter(pl.col("entry_type") == entry_type)

        return [self._parse_entry(row) for row in catalog_df.iter_rows(named=True)]

    def update_entry(self, entry_id: str, properties: Dict[str, Any]) -> bool:
        """
        Update an existing catalog entry using Delta Lake merge operation.

        Args:
            entry_id: ID of the entry to update
            properties: New properties to set

        Returns:
            bool: True if the entry was updated, False if not found
        """
        # Get current entry to merge properties
        current_entry = self.get_entry(entry_id)
        if not current_entry:
            return False

        # Update properties
        current_props = current_entry.model_dump()
        core_fields = ["id", "name", "entry_type", "created_at", "updated_at"]
        for field in core_fields:
            current_props.pop(field, None)
        current_props.update(properties)

        # Create update dataframe
        update_df = pl.DataFrame(
            [
                {
                    "id": entry_id,
                    "updated_at": datetime.now(),
                    "properties": json.dumps(current_props),
                }
            ]
        )

        # Perform merge operation
        update_df.write_delta(
            self.catalog_path,
            mode="merge",
            delta_merge_options={
                "predicate": "s.id = t.id",
                "source_alias": "s",
                "target_alias": "t",
            },
            storage_options=self.storage_options,
        ).when_matched_update_all().execute()

        return True

    def delete_entry(self, entry_id: str) -> bool:
        """
        Delete an entry from the catalog.

        Args:
            entry_id: ID of the entry to delete

        Returns:
            bool: True if the entry was deleted, False if not found
        """
        dt = DeltaTable(self.catalog_path, storage_options=self.storage_options)
        catalog_df = pl.read_delta(dt)

        # Filter out the entry
        filtered_df = catalog_df.filter(pl.col("id") != entry_id)

        if len(filtered_df) == len(catalog_df):
            # No rows were removed
            return False

        # Write back to catalog
        filtered_df.write_delta(
            self.catalog_path,
            mode="overwrite",
            storage_options=self.storage_options,
        )

        return True

    def get_or_create_timelake_config(
        self,
        timestamp_column: str,
        preprocessor: BaseTimeLakePreprocessor,
        storage: BaseTimeLakeStorage,
    ) -> Tuple[str, TimeLakeEntry]:
        """
        Get an existing TimeLake configuration or create a new one.
        """
        configs = self.list_entries(entry_type=CatalogEntryType.TIMELAKE_CONFIG.value)
        if configs:
            return configs[0].id, configs[0]

        # Create a new configuration
        config = TimeLakeEntry(
            timestamp_column=timestamp_column,
            timestamp_partition_column=preprocessor.get_timestamp_partition_column(
                timestamp_column
            ),
            partition_by=preprocessor.get_default_partitions(timestamp_column),
            timelake_id=str(uuid.uuid4()),
            timelake_storage=storage.__class__.__name__,
            timelake_preprocessor=preprocessor.__class__.__name__,
            storage_type=storage.__class__.__name__,
            name=os.path.basename(self.path),
        )
        config_id = self.add_entry(config)
        return config_id, config

    def create_dataset(
        self,
        name: str,
        path: Path,
        df: pl.DataFrame,
        partition_columns: Optional[List[str]] = None,
        primary_key: Optional[str] = None,
        storage_options: Optional[Dict[str, Any]] = None,
    ) -> DatasetEntry:
        """
        Create a new dataset in the catalog.

        Args:
            name: Name of the dataset
            path: Path to store the dataset
            df: Data to write
            partition_columns: Columns to partition by
            primary_key: Primary key column
            storage_options: Storage options for writing the dataset

        Returns:
            DatasetEntry: The created dataset entry
        """
        # Define partition columns if not provided
        if not partition_columns:
            partition_columns = []

        # Create dataset entry
        dataset_entry = DatasetEntry(
            name=name,
            path=str(path),
            dataset_schema={
                col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)
            },
            partition_columns=partition_columns,
            primary_key=primary_key,
        )

        # Add the dataset entry to the catalog
        self.add_entry(dataset_entry)

        # Write the dataset to the specified path
        df.write_delta(
            path,
            mode="overwrite",
            delta_write_options={"partition_by": partition_columns}
            if partition_columns
            else {},
            storage_options=storage_options,
        )

        return dataset_entry

    def get_timelake_config(self) -> Optional[TimeLakeEntry]:
        """
        Retrieve the TimeLake configuration entry from the catalog.

        Returns:
            Optional[TimeLakeEntry]: The TimeLake configuration if found, else None.
        """
        dt = DeltaTable(self.catalog_path, storage_options=self.storage_options)
        catalog_df = pl.read_delta(dt)

        # Filter for the TimeLake configuration entry
        result = catalog_df.filter(
            pl.col("entry_type") == CatalogEntryType.TIMELAKE_CONFIG.value
        )
        if len(result) == 0:
            return None

        # Parse the first matching entry as a TimeLakeEntry
        return self._parse_entry(result.row(0, named=True))  # Typed as TimeLakeEntry
