import os
import re
import warnings
from upath import UPath
from pathlib import Path
import shutil
from typing import Generator

from akutils.console_color import print_orange


def _correct_azure_path(file_path: Path | UPath) -> Path | UPath:
    """
    Due newer glob version, Azurepath is duplicate in UPath.glob() output

    Regex explained
    ===============
    abfs:/ -> start with absf:/
    [^/] -> n'est pas suivi par "/"
    .*$" -> tout ce qui suit
    """
    if "/abfs:/" in str(file_path):
        file_name = file_path.name
        file_path = UPath(re.sub(r"\/abfs:\/[^\/].*$", "", str(file_path))) / file_name
    return file_path


def list_files_from_dir(
    dir_path: Path | UPath,
    regex: str = r".*",
    case_sensitive: bool = False
) -> list[Path | UPath]:
    """
    Lists all file paths matching a regular expression in a directory.

    Parameters
    ----------
    dir_path : Path | UPath
        Path of the directory to be scanned
    regex : str, default r".*"
        Regex string to match files to be returned
        Default behaviour lists all files founded in the directory
    case_sensitive : bool, default False
        Allow to enable or disable case sensitive on regex match
    """
    flags = 0 if case_sensitive else re.IGNORECASE
    all_path: Generator[Path | UPath, None, None] = dir_path.glob("*")
    matched_files = [
        corrected_path for path in all_path
        if (corrected_path := _correct_azure_path(path)).is_file()
        and re.search(pattern=regex, string=corrected_path.name, flags=flags)
    ]
    return matched_files


def list_dir_from_dir(
    dir_path: Path | UPath,
    regex: str = r".*",
    case_sensitive: bool = False
) -> list[Path | UPath]:
    """
    Lists all directory paths matching a regular expression in a directory.

    Parameters
    ----------
    dir_path : Path | UPath
        Path of the directory to be scanned
    regex : str, default r".*"
        Regex string to match files to be returned
        Default behaviour lists all files founded in the directory
    case_sensitive : bool, default False
        Allow to enable or disable case sensitive on regex match
    """
    flags = 0 if case_sensitive else re.IGNORECASE
    all_path: Generator[Path | UPath, None, None] = dir_path.glob("*")
    matched_dirs = [
        corrected_path for path in all_path
        if (corrected_path := _correct_azure_path(path)).is_dir()
        and re.search(pattern=regex, string=corrected_path.name, flags=flags)
    ]
    return matched_dirs


def create_new_dir(
    dir_path: Path | UPath,
    force: bool = False
):
    """
    Create new directory.

    Parameters
    ----------
    dir_path : Path | UPath
        Path of the new directory
    force : bool, default False
        Use force=True to delete the current directory if it already exists
    """
    # if dir already exist and force disable : return error
    if dir_path.is_dir() and not force:
        raise IsADirectoryError("Directory already exist, use force=True to remove it")
    # if dir already exist and force enable : remove dir
    if dir_path.is_dir() and force:
        print("=> REMOVE existing directory")
        shutil.rmtree(dir_path)
    # create new dir
    UPath(dir_path).mkdir(parents=True, exist_ok=False)


def remove_files_from_directory(
    dir_path: Path | UPath,
    regex: str = r"*.parquet"
):
    """
    Create new directory.

    Parameters
    ----------
    dir_path : Path | UPath
        Path of the directory to be scanned
    regex : str, default r".*"
        Regex string to match the files to be removed.
        Default behaviour lists only .parquet files founded in the directory
    """
    file_list = UPath(dir_path).glob(regex)
    for file in file_list:
        os.remove(file)


def warn(message: str):
    """
    Warn in orange color
    """

    # Save the original warnings.warn function
    original_warn = warnings.warn

    # Temporarily override warnings.warn with print_orange
    warnings.warn = lambda message, *args, **kwargs: print_orange(message)

    # Call the overridden warning function
    warnings.warn(message=message)

    # Restore the original warnings.warn
    warnings.warn = original_warn
