import os
import sys


def get_default_config():
    """Get the default configuration file name.
    This should be called by a console script. We assume that the
    console script lives in the 'bin' dir of a sandbox or buildout, and
    that the dev.ini file lives in the 'etc' directory of the sandbox.
    """
    me = sys.argv[0]
    me = os.path.abspath(me)
    sandbox = os.path.dirname(os.path.dirname(me))
    config = os.path.join(sandbox, "etc", "dev.ini")
    return os.path.abspath(os.path.normpath(config))


truthy = frozenset(("t", "true", "y", "yes", "1"))


def asbool(s):
    if s is None:
        return False
    if isinstance(s, bool):
        return s
    s = str(s).strip()
    return s.lower() in truthy
