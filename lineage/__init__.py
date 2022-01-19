import os
import sys
from asyncore import compact_traceback
from ._version import version_info, __version__


def lineage_path():
    # Find folder that this module is contained in
    module = sys.modules[__name__]
    return os.path.dirname(os.path.abspath(module.__file__))


def error_traceback():
    (file, fun, line), t, v, tbinfo = compact_traceback()
    return "{} {} {}".format(t, v, tbinfo)
