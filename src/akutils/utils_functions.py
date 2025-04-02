import pandas as pd
from functools import wraps
from datetime import datetime

from akutils.os import warn


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        print(f'=> Start: {func.__name__}')
        start_time = datetime.now()
        result = func(*args, **kwargs)
        total_time = datetime.now() - start_time
        print(f'   End: {func.__name__} Took {total_time}')
        return result
    return timeit_wrapper


def contruct_function_args_from_locals(function, locals_args):
    specified_args = {
        key: value for key, value in locals_args.items() if key not in ["kwargs"]
    }
    additionnal_kwargs = locals_args["kwargs"]
    all_args = dict(specified_args, **additionnal_kwargs)

    # Filter on function alloewed args
    function_args = {
        key: value
        for key, value in all_args.items()
        if key in function.__code__.co_varnames
    }
    return function_args


def sanitize_function_args_from_locals(function, locals_args):
    # Check if a function is passed, if not return empty dict
    if not hasattr(function, '__call__'):
        return dict()

    flatten_argument = dict()
    if "kwargs" in locals_args.keys():
        flatten_argument.update(locals_args["kwargs"])

    flatten_argument.update({
        key: locals_args[key] for key in locals_args.keys()
        if key not in ["kwargs"]
    })

    # Filter on function allowed args
    function_args = {
        key: flatten_argument[key] for key in flatten_argument.keys()
        if key in function.__code__.co_varnames
    }
    return function_args


def control_if_usecols_exist_in_df(**read_csv_args):
    if "usecols" not in read_csv_args:
        return read_csv_args
    read_csv_args_header = read_csv_args.copy()
    read_csv_args_header.update({
        "usecols": None,
        "nrows": 0,
        "chunksize": None
    })
    df_columns = pd.read_csv(**read_csv_args_header).columns

    valid_usecols = []
    for col in read_csv_args["usecols"]:
        if col not in df_columns:
            warn(f"{col} (selected from usecols) was not found in df")
            continue
        valid_usecols.append(col)

    read_csv_args.update({"usecols": valid_usecols})
    return read_csv_args
