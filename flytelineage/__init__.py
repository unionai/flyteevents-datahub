from asyncore import compact_traceback
from ._version import version_info, __version__

def error_traceback():
    (file, fun, line), t, v, tbinfo = compact_traceback()
    return "{} {} {}".format(t, v, tbinfo)
