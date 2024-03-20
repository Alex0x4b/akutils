import os
import re
from upath import UPath
from pathlib import Path
import shutil


def list_files_from_dir(
    dir_path: Path | UPath,
    regex: str = r".*",
    case_sensitive: bool = False
):
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
    all_files = dir_path.glob("*")
    matched_files = [file for file in all_files
                     if re.search(pattern=regex, string=file.name, flags=flags)]
    return matched_files


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
