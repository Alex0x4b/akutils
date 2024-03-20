import pandas as pd
from typing import Callable
from pandas._typing import (
    FilePath,
    ReadCsvBuffer,
    DtypeArg
)
from akutils.utils_functions import timeit, sanitize_function_args_from_locals


@timeit
def read_csv_in_chunks(
    filepath_or_buffer: FilePath | ReadCsvBuffer[bytes] | ReadCsvBuffer[str],
    chunk_func: Callable | None = None,
    chunksize: int = 10**6,
    dtype: DtypeArg | None = "string",
    **kwargs: dict | None
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
    **kwargs : dict | None
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
    print(f"File: {filepath_or_buffer}")
    locals_args = locals()  # get all args passed in the function
    read_csv_args = sanitize_function_args_from_locals(pd.read_csv, locals_args)

    df = pd.DataFrame()
    counter = 0
    for chunk in pd.read_csv(**read_csv_args):
        print(f"Chunk number => {counter}")
        chunk_func_kwarg = sanitize_function_args_from_locals(chunk_func, locals_args)
        chunk = chunk_func(chunk, **chunk_func_kwarg) if chunk_func else chunk
        df = pd.concat([df, chunk], axis=0, ignore_index=True)
        counter += 1
    return df
