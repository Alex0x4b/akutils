import pandas as pd
from pandas.api.types import is_float_dtype


def fillna_float_columns(
    df: pd.DataFrame,
    filler: float = 0
) -> pd.DataFrame:
    cols_float = [
        col for col in df
        if is_float_dtype(df[col])
    ]
    df[cols_float] = df[cols_float].fillna(filler)
    return df
