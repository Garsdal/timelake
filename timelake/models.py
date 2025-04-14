from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from timelake.constants import TIMELAKE_VERSION, TimeLakeColumns


class TimeLakeMetadata(BaseModel):
    timestamp_column: str
    partition_by: List[str]
    timelake_id: str
    timelake_storage: str
    timelake_preprocessor: str
    timelake_version: str = TIMELAKE_VERSION
    inserted_at_column: str = TimeLakeColumns.INSERTED_AT.value
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
