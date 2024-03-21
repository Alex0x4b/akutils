from functools import wraps
from datetime import datetime


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
