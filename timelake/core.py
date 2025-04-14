# timelake/core.py
import json
import os
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional, Union

import polars as pl
from deltalake import DeltaTable, write_deltalake


class TimeFeature:
    """Base class for time series features in TimeLake."""

    @classmethod
    def lag(
        cls,
        name: str,
        source_column: str,
        periods: int,
        fill_value: Optional[Any] = None,
        cache_policy: Literal["on_read", "on_write"] = "on_read",
    ) -> "TimeFeature":
        """
        Create a lag feature.

        Args:
            name: Name of the feature
            source_column: Column to apply lag to
            periods: Number of periods to lag
            fill_value: Value to use for filling NAs (None = NaN)
            cache_policy: Whether to cache on read or write

        Returns:
            TimeFeature instance
        """
        feature = cls()
        feature.type = "lag"
        feature.name = name
        feature.source_column = source_column
        feature.periods = periods
        feature.fill_value = fill_value
        feature.cache_policy = cache_policy
        feature.created_at = datetime.now().isoformat()
        feature.updated_at = feature.created_at
        return feature

    def to_dict(self) -> Dict[str, Any]:
        """Convert feature to dictionary for serialization."""
        return self.__dict__

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TimeFeature":
        """Create feature from dictionary."""
        feature = cls()
        for key, value in data.items():
            setattr(feature, key, value)
        return feature

    def compute(self, df: pl.DataFrame, timestamp_column: str) -> pl.DataFrame:
        """
        Compute this feature on the provided dataframe.

        Args:
            df: Input dataframe
            timestamp_column: Timestamp column name

        Returns:
            DataFrame with feature added
        """
        if self.type == "lag":
            sorted_df = df.sort(timestamp_column)
            lagged = sorted_df.with_columns(
                pl.col(self.source_column).shift(self.periods).alias(self.name)
            )

            if self.fill_value is not None:
                lagged = lagged.with_columns(
                    pl.col(self.name).fill_null(self.fill_value)
                )

            return lagged

        raise ValueError(f"Unknown feature type: {self.type}")


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
    def create(
        cls, path: str, timestamp_column: str, partition_by: Optional[List[str]] = None
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
        metadata = {
            "timelake_version": "0.1.0",
            "created_at": datetime.now().isoformat(),
            "timestamp_column": timestamp_column,
            "partition_by": partition_by or [],
            "lake_id": str(uuid.uuid4()),
        }

        # Create an empty Delta table
        empty_df = pl.DataFrame({timestamp_column: []})

        # Write empty dataframe to initialize the table
        write_deltalake(path, empty_df.to_pandas(), partition_by=partition_by or [])

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
        insertion_time: Optional[datetime] = None,
    ) -> str:
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

        # Add insertion time
        now = insertion_time or datetime.now()
        df_with_metadata = df.with_columns(
            pl.lit(now.isoformat()).alias("_insertion_time")
        )

        # Write to Delta table
        result = write_deltalake(
            self.path,
            df_with_metadata.to_pandas(),
            mode=mode,
            partition_by=self.metadata.get("partition_by", []),
        )

        # Return the version
        return result["version"]

    def read(
        self,
        start_time: Optional[Union[str, datetime]] = None,
        end_time: Optional[Union[str, datetime]] = None,
        features: Optional[List[str]] = None,
        compute_if_missing: bool = True,
        horizon: Optional[Union[str, timedelta]] = None,
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

        # Create a filter based on time range
        filters = []
        if start_time:
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time)
            filters.append((self.timestamp_column, ">=", start_time.isoformat()))

        if end_time:
            if isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)
            filters.append((self.timestamp_column, "<=", end_time.isoformat()))

        # Apply horizon if specified to prevent data leakage
        if horizon:
            if isinstance(horizon, str):
                # Parse string like "1d", "3h", etc.
                unit = horizon[-1]
                value = int(horizon[:-1])
                if unit == "d":
                    horizon = timedelta(days=value)
                elif unit == "h":
                    horizon = timedelta(hours=value)
                elif unit == "m":
                    horizon = timedelta(minutes=value)
                else:
                    raise ValueError(f"Unsupported horizon unit: {unit}")

            # Calculate cutoff timestamp
            cutoff_time = end_time - horizon if end_time else datetime.now() - horizon
            filters.append(("_insertion_time", "<=", cutoff_time.isoformat()))

        # Read the data
        pdf = dt.to_pandas(filters=filters)
        df = pl.from_pandas(pdf)

        # If no features requested, return the base data
        if not features:
            return df

        # Process requested features
        for feature_name in features:
            # Check if feature already exists in data
            if feature_name in df.columns:
                continue

            # Compute feature
            if compute_if_missing:
                feature = self.get_feature_info(feature_name)
                if feature:
                    df = feature.compute(df, self.timestamp_column)
                else:
                    raise ValueError(f"Feature '{feature_name}' not found")
            else:
                # Skip if not computing missing features
                continue

        return df

    def add_feature(self, feature: TimeFeature) -> bool:
        """
        Add a feature to the TimeLake.

        Args:
            feature: TimeFeature to add

        Returns:
            True if successful
        """
        # Check if feature already exists
        feature_path = os.path.join(self.features_path, f"{feature.name}.json")
        if os.path.exists(feature_path):
            raise ValueError(f"Feature '{feature.name}' already exists")

        # Save feature definition
        with open(feature_path, "w") as f:
            json.dump(feature.to_dict(), f, indent=2)

        return True

    def list_features(self) -> List[str]:
        """
        List all registered features.

        Returns:
            List of feature names
        """
        features = []
        for filename in os.listdir(self.features_path):
            if filename.endswith(".json"):
                features.append(filename[:-5])  # Remove .json extension
        return features

    def get_feature_info(self, feature_name: str) -> Optional[TimeFeature]:
        """
        Get information about a specific feature.

        Args:
            feature_name: Name of the feature

        Returns:
            TimeFeature if found, None otherwise
        """
        feature_path = os.path.join(self.features_path, f"{feature_name}.json")
        if not os.path.exists(feature_path):
            return None

        with open(feature_path, "r") as f:
            feature_data = json.load(f)

        return TimeFeature.from_dict(feature_data)

    def update_feature(self, feature_name: str, **kwargs) -> bool:
        """
        Update a feature definition.

        Args:
            feature_name: Name of the feature to update
            **kwargs: Attributes to update

        Returns:
            True if successful
        """
        feature = self.get_feature_info(feature_name)
        if not feature:
            raise ValueError(f"Feature '{feature_name}' not found")

        # Update attributes
        for key, value in kwargs.items():
            if hasattr(feature, key):
                setattr(feature, key, value)
            else:
                raise ValueError(
                    f"Invalid attribute '{key}' for feature {feature_name}"
                )

        # Update timestamp
        feature.updated_at = datetime.now().isoformat()

        # Save updated feature
        feature_path = os.path.join(self.features_path, f"{feature_name}.json")
        with open(feature_path, "w") as f:
            json.dump(feature.to_dict(), f, indent=2)

        return True

    def remove_feature(self, feature_name: str) -> bool:
        """
        Remove a feature.

        Args:
            feature_name: Name of the feature to remove

        Returns:
            True if successful
        """
        feature_path = os.path.join(self.features_path, f"{feature_name}.json")
        if not os.path.exists(feature_path):
            raise ValueError(f"Feature '{feature_name}' not found")

        # Remove feature definition file
        os.remove(feature_path)

        return True
