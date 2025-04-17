from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from timelake.constants import TIMELAKE_VERSION, TimeLakeColumns, TimeLakeStorageType


class TimeLakeMetadata(BaseModel):
    timestamp_column: str
    timestamp_partition_column: str
    partition_by: List[str]
    timelake_id: str
    timelake_storage: str
    timelake_preprocessor: str
    timelake_version: str = TIMELAKE_VERSION
    inserted_at_column: str = TimeLakeColumns.INSERTED_AT.value
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    storage_type: str = Field(default=TimeLakeStorageType.LOCAL.value)  # New field
