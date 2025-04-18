from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field  # Import ConfigDict

from timelake.constants import (
    TIMELAKE_VERSION,
    CatalogEntryType,
    StorageType,
    TimeLakeColumns,
)


class BaseCatalogEntry(BaseModel):
    """
    Base class for all catalog entries.
    All concrete catalog entries should inherit from this class.
    """

    id: Optional[str] = None  # Will be assigned by the catalog manager
    name: str
    entry_type: str  # This should match a value in CatalogEntryType enum
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

    model_config = ConfigDict(extra="allow")


class TimeLakeEntry(BaseCatalogEntry):
    """
    Configuration for a TimeLake instance.
    Stored as an entry in the catalog table.
    """

    timestamp_column: str
    timestamp_partition_column: str
    partition_by: List[str]
    timelake_id: str
    timelake_storage: str  # Storage class name
    timelake_preprocessor: str  # Preprocessor class name
    timelake_version: str = TIMELAKE_VERSION
    inserted_at_column: str = TimeLakeColumns.INSERTED_AT.value
    storage_type: str = Field(default=StorageType.LOCAL.value)

    def __init__(self, **data):
        data["entry_type"] = CatalogEntryType.TIMELAKE_CONFIG.value
        data.setdefault("name", "timelake")
        super().__init__(**data)


class DatasetEntry(BaseCatalogEntry):
    """
    Configuration for a dataset in the TimeLake.
    Stored as an entry in the catalog table.
    """

    path: str
    dataset_schema: Dict[str, str]  # Renamed from `schema` to `dataset_schema`
    partition_columns: List[str] = Field(default_factory=list)
    primary_key: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data):
        data["entry_type"] = CatalogEntryType.DATASET.value
        super().__init__(**data)


class TimeLakeCatalogSchema(BaseModel):
    """
    Schema definition for the TimeLakeCatalog delta table.
    This model defines the structure of the Delta table that stores catalog entries.
    It is not stored in the catalog itself but used to create the catalog table.
    """

    id: str
    name: str
    entry_type: str
    created_at: datetime
    updated_at: datetime
    properties: str  # JSON serialized properties
