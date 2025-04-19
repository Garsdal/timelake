from enum import Enum

TIMELAKE_VERSION = "0.0.1"
CATALOG_TABLE_NAME = "_timelake_catalog"
DATASETS_FOLDER = "_timelake_datasets"  # Add this new constant


class TimeLakeColumns(Enum):
    """
    Enum for TimeLake default column names.
    """

    INSERTED_AT = "inserted_at"
    SIGNAL = "signal"


class StorageType(Enum):
    LOCAL = "local"
    S3 = "s3"


class CatalogEntryType(Enum):
    """
    Enum for catalog entry types.
    """

    TIMELAKE_CONFIG = "timelake_config"
    DATASET = "dataset"
