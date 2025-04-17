from enum import Enum

TIMELAKE_VERSION = "0.0.1"


class TimeLakeColumns(Enum):
    """
    Enum for TimeLake default column names.
    """

    INSERTED_AT = "inserted_at"
    SIGNAL = "signal"
