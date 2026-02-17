import pandas as pd
import fsspec  # type: ignore
import re
import zipfile
from io import TextIOWrapper, BytesIO
from upath import UPath
from upath.implementations.cloud import AzurePath
from pathlib import Path
from typing import Callable
from pandas._typing import (
    FilePath,
    ReadCsvBuffer,
    DtypeArg
)
from akutils.utils_functions import (
    timeit,
    contruct_function_args_from_locals,
)
from akutils.os import list_files_from_dir, warn


def _warn_column_mismatch(list_of_df):
    for i, df in enumerate(list_of_df[1:], start=1):
        diff = set(df.columns) ^ set(list_of_df[0].columns)
        if diff:
            extra = set(df.columns) - set(list_of_df[0].columns)
            missing = set(list_of_df[0].columns) - set(df.columns)
            warn(f"[WARNING] Columns mismatch for file_{i + 1}: missing={missing or '{}'}, extra={extra or '{}'}")


@timeit
def read_csv_in_chunks(
    filepath_or_buffer: FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str],
    chunk_func: Callable | None = None,
    chunk_func_kwarg=None,
    chunksize: int = 10**6,
    dtype: DtypeArg | None = "string",
    **kwargs
) -> pd.DataFrame:
    """
    Read in chunks general delimited file into DataFrame (based on pd.read_csv)

    Custom function could be applied to each chunk. It could for example be use to apply
    filter at reading level and preserve memory usage on big DataFrame.

    Parameters
    ----------
    filepath_or_buffer : str, path object or file-like object
        Any valid string path is acceptable. The string could be a URL.
    chunk_func : Callable, default None
        The function will be applied to each chunk (e.g. filter, change type...)
        first function arg should should be the chunk df
    **kwargs
        Pass any argument allowed by pd.read_csv and/or by the custom chunk function

    Exemple chunk usage
    -------------------

    .. code-block:: python

        import akutils as ak
        from akutils import PATH_TO_AKUTILS_PKG

        def filter_on_countries(df, countries):
            return df[df["country"].isin(countries)]

        file_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales.csv"
        df = ak.read_csv_in_chunks(
            filepath_or_buffer=file_path,
            sep=";",
            chunk_func=filter_on_countries,
            countries=["France", "Germany"],
            chunksize=5
        )
    """
    if chunk_func_kwarg is None:
        chunk_func_kwarg = {}
    print(f"File: {filepath_or_buffer}")
    locals_args = locals()  # get all args passed in the function
    read_csv_args = contruct_function_args_from_locals(pd.read_csv, locals_args)

    df = pd.DataFrame()
    counter = 0
    for chunk in pd.read_csv(**read_csv_args):
        print(f"Chunk number => {counter}")
        chunk = chunk_func(df=chunk, **chunk_func_kwarg) if chunk_func else chunk
        df = pd.concat([df, chunk], axis=0, ignore_index=True)
        counter += 1
    return df


@timeit
def read_multiple_csv_from_zip(
    zip_path: Path | UPath | BytesIO,
    regex: str = r".*",
    case_sensitive: bool = False,
    allowed_extension: list = [".csv", ".txt", ".dsv", ".gz", ".zip", ".tar", "7z"],
    add_source: bool = False,
    **kwargs
):
    """
    From a given zip, lists, reads and concatenates into a DataFrame all
    delimited files matching the requesting pattern.

    It uses pd.read_csv and ak.read_csv_in_chunks. You can use any parameters of those
    functions.

    Parameters
    ----------
    zip_path : Path | UPath
        Path of the directory to be scanned
    regex : str, default r".*"
        Regex string to select from the directory only the files mathcing the pattern
        Default behaviour lists all files founded in the directory
    case_sensitive : bool, default False
        Allow to enable or disable case sensitive on regex match
    allowed_extension : list, default [".csv", ".txt", ".dsv", ".gz", ".zip", ".tar"]
    **kwargs
        Pass any argument allowed by pd.read_csv and/or by the custom chunk function
    """

    # Handle Azure zip
    if type(zip_path) is AzurePath:
        with fsspec.open(zip_path, 'r') as f:
            zip_path = BytesIO(f.read())

    # Get first mathing zip from a directory
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get list of file names in the archive
        file_list = zip_ref.namelist()

        # Find files matching the pattern in zip
        flags = 0 if case_sensitive else re.IGNORECASE
        matching_files = [f for f in file_list if re.search(regex, f, flags=flags)]

        # Filter on files with csv extension
        allowed_extension = [ext.lower() for ext in allowed_extension]
        files_allowed = [
            file for file in matching_files
            if UPath(file).suffix.lower() in allowed_extension
        ]

        if not files_allowed:
            warn(f"No files matching pattern '{regex}' found in {zip_path}")

        if len(files_allowed) > 1:
            print(
                f"[INFO] Multiple files matching pattern '{regex}' found in {zip_path.name}."
                f"\nTry to concat those {len(files_allowed)}"
            )

        # Get encoding
        encoding = kwargs.get("encoding") if kwargs.get("encoding") else "utf-8"

        # Load files
        list_of_df = []
        for file_name in files_allowed:
            print(f"=> from ZIP READ: {file_name}")
            with zip_ref.open(file_name) as file:
                text_file = TextIOWrapper(file, encoding=encoding)
                _df = read_csv_in_chunks(filepath_or_buffer=text_file, **kwargs)
                if add_source:
                    _df["file_source"] = file.name
                list_of_df.append(_df)

        _warn_column_mismatch(list_of_df)
        df = pd.concat(list_of_df, axis=0, ignore_index=True)
    return df


@timeit
def read_multiple_csv_from_dir(
    dir_path: Path | UPath,
    regex: str = r".*",
    case_sensitive: bool = False,
    allowed_extension: list = [".csv", ".txt", ".dsv", ".gz", ".zip", ".tar", "7z"],
    add_source: bool = False,
    **kwargs
):
    """
    From a given directory, lists, reads and concatenates into a DataFrame all
    delimited files matching the requesting pattern.

    It uses pd.read_csv and ak.read_csv_in_chunks. You can use any parameters of those
    functions.

    Parameters
    ----------
    dir_path : Path | UPath
        Path of the directory to be scanned
    regex : str, default r".*"
        Regex string to select from the directory only the files mathcing the pattern
        Default behaviour lists all files founded in the directory
    case_sensitive : bool, default False
        Allow to enable or disable case sensitive on regex match
    allowed_extension : list, default [".csv", ".txt", ".dsv", ".gz", ".zip", ".tar"]
    **kwargs
        Pass any argument allowed by pd.read_csv and/or by the custom chunk function
    """
    # Lists all files matching regex
    file_matched = list_files_from_dir(
        dir_path=dir_path, regex=regex, case_sensitive=case_sensitive)
    # Filter on files with csv extension
    allowed_extension = [ext.lower() for ext in allowed_extension]
    files_allowed = [
        file for file in file_matched
        if file.suffix.lower() in allowed_extension
    ]

    list_of_df = []
    if len(files_allowed) == 0:
        warn(
            f"No file found in {dir_path}: empty pd.DataFrame has been returned")
        return pd.DataFrame
    for file in files_allowed:
        print(f"READ: {file.name}")
        _df = read_csv_in_chunks(filepath_or_buffer=file, **kwargs)
        if add_source:
            _df["file_source"] = file.name
        list_of_df.append(_df)
    _warn_column_mismatch(list_of_df)
    df = pd.concat(list_of_df, axis=0, ignore_index=True)
    return df


@timeit
def read_multiple_xlsx_from_dir(
    dir_path: Path | UPath,
    regex: str = r".*",
    case_sensitive: bool = False,
    allowed_extension: list = [".xlsx", ".xls", ".xlsm", ".xlsb"],
    add_source: bool = False,
    **kwargs
):
    """
    From a given directory, lists, reads and concatenates into a DataFrame all
    Excel files matching the requesting pattern.

    It uses pd.read_excel, you can use any of its parameters

    Parameters
    ----------
    dir_path : Path | UPath
        Path of the directory to be scanned
    regex : str, default r".*"
        Regex string to select from the directory only the files mathcing the pattern
        Default behaviour lists all files founded in the directory
    case_sensitive : bool, default False
        Allow to enable or disable case sensitive on regex match
    allowed_extension : list, default [".xlsx", ".xls", ".xlsm", ".xlsb"]
    **kwargs
        Pass any argument allowed by pd.read_excel
    """
    # Lists all files matching regex
    file_matched = list_files_from_dir(
        dir_path=dir_path, regex=regex, case_sensitive=case_sensitive)
    # Filter on files with csv extension
    allowed_extension = [ext.lower() for ext in allowed_extension]
    files_allowed = [
        file for file in file_matched
        if file.suffix.lower() in allowed_extension
    ]

    list_of_df = []
    if len(files_allowed) == 0:
        warn(
            f"No file found in {dir_path}: empty pd.DataFrame has been returned")
        return pd.DataFrame
    for file in files_allowed:
        print(f"READ: {file.name}")
        _df = pd.read_excel(io=file, **kwargs)
        if add_source:
            _df["file_source"] = file.name
        list_of_df.append(_df)
    _warn_column_mismatch(list_of_df)
    df = pd.concat(list_of_df, axis=0, ignore_index=True)
    return df
