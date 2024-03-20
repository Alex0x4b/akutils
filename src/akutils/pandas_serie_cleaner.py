import pandas as pd
import warnings


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
        List of column names for which special accents and brackets must be removed
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
