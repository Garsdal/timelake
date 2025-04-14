from enum import Enum

TIMELAKE_VERSION = "0.0.1"


class TimeLakeColumns(Enum):
    """
    Enum for TimeLake defauly column names.
    """

    INSERTED_AT = "_inserted_at"
