# Import API
from akutils.__api__ import *

# Get path to akutils pkg to import test fixtures
from pathlib import Path
from pkg_resources import resource_filename

PATH_TO_AKUTILS_PKG = Path(resource_filename(__name__, "/"))

# module level doc-string
__docformat__ = "restructuredtext"
__doc__ = """
utils for py general manipulations
==================================
"""
