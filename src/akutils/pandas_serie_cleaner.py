import pandas as pd
from pandas.api.types import (
    is_string_dtype,
    is_object_dtype,
    is_float_dtype,
    is_datetime64_dtype
)
from typing import Optional
import warnings

# https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
pd.options.mode.copy_on_write = True


def capitalise_cols(
    df: pd.DataFrame,
    cols: list
) -> pd.DataFrame:
    """
    Converts selected DataFrame columns to uppercase.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be modified
    cols : list
        List of column names to be capitalized
    """
    for col in cols:
        if col not in df.columns:
            warnings.warn(f"column not found in DataFrame: {col}")
            continue
        df[col] = (
            df[col]
            .astype(str)
            .fillna("")
            .str.upper()
        )
    return df


def remove_accent_from_cols(
    df: pd.DataFrame,
    cols: list
) -> pd.DataFrame:
    """
    Remove accents and special characters from selected DataFrame columns

    Based on:
    https://stackoverflow.com/questions/37926248/how-to-remove-accents-from-values-in-columns

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be modified
    cols : list
        List of column names for which special accents and brackets should be removed
    """
    for col in cols:
        if col not in df.columns:
            warnings.warn(f"column not found in DataFrame: {col}")
            continue
        df[col] = (
            df[col]
            .astype(str)
            .fillna("")
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
        )
    return df


def strip_columns(
    df: pd.DataFrame,
    cols: Optional[list] = None
) -> pd.DataFrame:
    """
    Strip selected DataFrame columns.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to be modified
    cols : list, default None
        List of column names to be striped
    """
    cols = list(df.columns) if not cols else cols
    for column in cols:
        if column not in df.columns:
            warnings.warn(f"column not found in DataFrame: {column}")
            continue
        if (not is_string_dtype(df[column])) & (not is_object_dtype(df[column])):
            continue
        df.loc[(~df[column].isna()), column] = df[column].str.strip()
    return df


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


def convert_datetimes_to_date(df: pd.DataFrame) -> pd.DataFrame:
    cols_datetime = [
        col for col in df
        if is_datetime64_dtype(df[col])
    ]
    for col in cols_datetime:
        df[col] = df[col].dt.date
    return df
