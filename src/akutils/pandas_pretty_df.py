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


def remove_empty_cols_from_df(df: pd.DataFrame) -> pd.DataFrame:
    filled_cols = [
        col for col in df.columns
        if (
            (~(df[col].isna().all()))
            & (~((df[col] == "").all()))
        )
    ]
    return df[filled_cols]
