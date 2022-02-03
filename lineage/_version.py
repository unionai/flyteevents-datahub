version_info = (
    0,
    0,
    2,
    # "b2",  # release (b1, rc1, or "" for final or dev)
    # "dev"  # dev or nothing
)

# pep 440 version: no dot before beta/rc, but before .dev
# 0.1.0rc1
# 0.1.0a1
# 0.1.0b1.dev
# 0.1.0.dev

__version__ = ".".join(map(str, version_info[:3])) + ".".join(version_info[3:])
