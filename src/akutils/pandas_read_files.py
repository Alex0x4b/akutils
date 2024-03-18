import pandas as pd
from akutils.utils_function import timeit, contruct_function_args_from_locals


@timeit
def read_csv_in_chunks(
    filepath_or_buffer,
    chunk_func=None,
    chunksize=10**6,
    dtype="string",
    **kwargs
):
    """
    Usage
    -----
    Use pd.read_csv in chunk and allow to apply a any custom function to each chunk
    e.g to preserve memory, you can pass a function which filter each chunk

    Arguments
    ---------
    filepath_or_buffer:
    chunk_func: the function will be applied to each chunk (e.g. filter, change type...)
        function should have "df" as first argument
    **kwargs: accept any argument from pd.read_csv and from the custom chunk function

    Exemple chunk usage
    -------------------

    def filter_on_countries(df, countries):
        return df[df["country"].isin(countries)]

    df = read_csv_in_chunks(
        filepath_or_buffer="path_to.csv",
        chunk_func=filter_on_adh,
        countries=["France", "Germany"],
    )
    """
    print(f"File: {filepath_or_buffer}")
    locals_args = locals()  # get all args passed in the function
    read_csv_args = contruct_function_args_from_locals(pd.read_csv, locals_args)

    df = pd.DataFrame()
    counter = 0
    for chunk in pd.read_csv(**read_csv_args):
        print(f"Chunk number => {counter}")
        chunk_func_kwarg = contruct_function_args_from_locals(chunk_func, locals_args)
        chunk = chunk_func(df=chunk, **chunk_func_kwarg) if chunk_func else chunk
        df = pd.concat([df, chunk], axis=0, ignore_index=True)
        counter += 1
    return df
