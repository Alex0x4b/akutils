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

    # Contruct agruments from local
    specified_args = {
        key: value for key, value in locals_args.items()
        if key not in ["kwargs"]
    }
    additionnal_kwargs = locals_args["kwargs"]
    all_args = dict(specified_args, **additionnal_kwargs)

    # Filter on function alloewed args
    function_args = {
        key: value for key, value in all_args.items()
        if key in function.__code__.co_varnames
    }
    return function_args
