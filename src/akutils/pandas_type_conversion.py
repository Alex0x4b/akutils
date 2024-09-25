import warnings
import pandas as pd

# https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
pd.options.mode.copy_on_write = True


def columns_to_float(
    df: pd.DataFrame,
    col_list: list,
    keep_source: bool = False
) -> pd.DataFrame:
    suffixe = "_flt" if keep_source else ""
    for column in col_list:
        if column not in df.columns:
            warnings.warn(f"column not found in DataFrame: {column}")
            continue
        new_col = f"{column}{suffixe}"
        df[new_col] = df[column].values.astype(str)
        had_percent_sign = df[new_col].str.contains("%")
        df[new_col] = (
            df[new_col]
            .str.replace(" ", "")  # nbsp (%A0). Needed for some CSV files
            .str.replace(" ", "")  # normal space (%20)
            .str.replace(",", ".")
            .str.replace("%", "")
        )
        df[new_col] = pd.to_numeric(df[new_col], errors="coerce")
        df[new_col] = df[new_col].fillna(0)
        df[new_col] = df[new_col].values.astype(float)
        df.loc[had_percent_sign, new_col] = df.loc[had_percent_sign, new_col] / 100
    return df


def columns_to_int(
    df: pd.DataFrame,
    col_list: list,
    keep_source: bool = False
) -> pd.DataFrame:
    suffixe = "_int" if keep_source else ""
    for column in col_list:
        if column not in df.columns:
            warnings.warn(f"column not found in DataFrame: {column}")
            continue
        new_col = f"{column}{suffixe}"
        df[new_col] = df[column].values.astype(str)
        df[new_col] = (
            df[new_col]
            .str.replace(" ", "")  # nbsp (%A0). Needed for some CSV files
            .str.replace(" ", "")  # normal space (%20)
            .str.replace(",", ".")
            .str.replace("%", "")
        )
        df[new_col] = pd.to_numeric(df[new_col], errors="coerce")
        df[new_col] = df[new_col].fillna(0)
        df[new_col] = df[new_col].values.astype(int)
    return df


def columns_to_date(
    df: pd.DataFrame,
    col_list: list,
    date_format: str = "%Y-%m-%d",
    keep_source: bool = False
) -> pd.DataFrame:
    # normalize date format separator
    date_format = date_format.replace("/", "-")
    # calculate date_format length in order to remove time if any
    date_len = (
        ("Y" in date_format) * 4
        + ("y" in date_format) * 2
        + ("m" in date_format) * 2
        + ("d" in date_format) * 2
        + ("-" in date_format) * 2
    )
    # convert to date
    suffixe = "_dt" if keep_source else ""
    for column in col_list:
        if column not in df.columns:
            warnings.warn(f"column not found in DataFrame: {column}")
            continue
        new_col = f"{column}{suffixe}"
        df[new_col] = df[column].astype(str)
        df[new_col] = df[new_col].str.replace("/", "-").str[: date_len]
        df[new_col] = pd.to_datetime(df[new_col], format=date_format, errors="coerce")
    return df
