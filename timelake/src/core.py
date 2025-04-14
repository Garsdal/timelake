# timelake/core.py
import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

import polars as pl
from deltalake import DeltaTable, write_deltalake


class TimeLake:
    """
    TimeLake main class for managing time series data using Delta Lake.
    """

    def __init__(self, path: str, timestamp_column: str, metadata: Dict[str, Any]):
        """
        Initialize a TimeLake instance.

        Args:
            path: Path to the Delta Lake location
            timestamp_column: Column name containing timestamps
            metadata: TimeLake metadata
        """
        self.path = path
        self.timestamp_column = timestamp_column
        self.metadata = metadata
        self.features_path = os.path.join(path, "_timelake_features")
        self._ensure_features_dir()

    def _ensure_features_dir(self):
        """Ensure the features directory exists."""
        os.makedirs(self.features_path, exist_ok=True)

    @classmethod
    def _add_inserted_at_column(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Add an inserted_at column to the DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with inserted_at column
        """
        now = datetime.now().isoformat()
        return df.with_columns(pl.lit(now).alias("_inserted_at"))

    @classmethod
    def create(
        cls,
        path: str,
        df: pl.DataFrame,
        timestamp_column: str,
        partition_by: Optional[List[str]] = None,
    ) -> "TimeLake":
        """
        Create a new TimeLake.

        Args:
            path: Path where to create the Delta Lake
            timestamp_column: Column name containing timestamps
            partition_by: Columns to partition by

        Returns:
            TimeLake instance
        """
        # Create directory if it doesn't exist
        os.makedirs(path, exist_ok=True)

        # Initialize metadata
        partition_by = [timestamp_column, *partition_by] or [timestamp_column]
        metadata = {
            "timelake_version": "0.1.0",
            "created_at": datetime.now().isoformat(),
            "timestamp_column": timestamp_column,
            "inserted_at_column": "_inserted_at",
            "partition_by": partition_by,
            "lake_id": str(uuid.uuid4()),
        }

        # Write empty dataframe to initialize the table
        write_deltalake(
            path, cls._add_inserted_at_column(df), partition_by=partition_by
        )

        # Create the Lake instance
        lake = cls(path, timestamp_column, metadata)

        # Save metadata
        lake._save_metadata()

        return lake

    @classmethod
    def open(cls, path: str) -> "TimeLake":
        """
        Open an existing TimeLake.

        Args:
            path: Path to the TimeLake

        Returns:
            TimeLake instance
        """
        metadata_path = os.path.join(path, "_timelake_metadata.json")
        if not os.path.exists(metadata_path):
            raise ValueError(f"No TimeLake found at {path}")

        with open(metadata_path, "r") as f:
            metadata = json.load(f)

        return cls(path, metadata["timestamp_column"], metadata)

    def _save_metadata(self):
        """Save TimeLake metadata to disk."""
        metadata_path = os.path.join(self.path, "_timelake_metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(self.metadata, f, indent=2)

    def write(
        self,
        df: pl.DataFrame,
        mode: Literal["append", "overwrite"] = "append",
    ) -> None:
        """
        Write data to the TimeLake.

        Args:
            df: DataFrame to write
            mode: Write mode (append or overwrite)
            insertion_time: Optional manual insertion time (defaults to now)

        Returns:
            Version identifier
        """
        # Validate that the timestamp column exists
        if self.timestamp_column not in df.columns:
            raise ValueError(
                f"Timestamp column '{self.timestamp_column}' not found in DataFrame"
            )

        df_ts = self._add_inserted_at_column(df)
        df_ts.write_delta(
            self.path,
            mode=mode,
            delta_write_options={
                "partition_by": self.metadata["partition_by"],
            },
        )

    def read(
        self,
    ) -> pl.DataFrame:
        """
        Read data from the TimeLake with optional feature computation.

        Args:
            start_time: Start of time range
            end_time: End of time range
            features: List of features to include
            compute_if_missing: Compute features if not cached
            horizon: Time horizon to consider for preventing data leakage

        Returns:
            DataFrame with requested data and features
        """
        # Open Delta table
        dt = DeltaTable(self.path)

        return pl.read_delta(dt)
