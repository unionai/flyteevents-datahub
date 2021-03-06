import os
from os.path import dirname, join as pjoin

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open("README.md") as f:
    long_description = f.read()

with open("CHANGES.txt") as f:
    changes = f.read()

# Get the current package version.
version_ns = {}
with open(pjoin(here, "flytelineage", "_version.py")) as f:
    exec(f.read(), {}, version_ns)


setup(
    name="flytelineage",
    version=version_ns["__version__"],
    description="Flyte Data Lineage Python App & Library",
    long_description=long_description + "\n\n" + changes,
    classifiers=[
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP",
    ],
    url="",
    keywords="Flyte Data Lineage",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points="""\
      [console_scripts]
      flytelineage = flytelineage.app:main
      """,
)
