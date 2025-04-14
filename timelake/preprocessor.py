from datetime import datetime

import polars as pl

from timelake.base import BaseTimeLakePreprocessor


class TimeLakePreprocessor(BaseTimeLakePreprocessor):
    def add_inserted_at_column(self, df: pl.DataFrame) -> pl.DataFrame:
        now = datetime.now().isoformat()
        return df.with_columns(pl.lit(now).alias("_inserted_at"))
