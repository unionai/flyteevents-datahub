from datetime import datetime
import typing
from hashlib import md5
import logging
import copy
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)


class DatasetSchema(object):
    """DatasetSchema infers a pyarrow schema from a dataset.
    PyArrows schema is used as its superior to Pandas.
    """

    def __init__(
        self,
        name,
        description="",
        created=None,
        last_modified=None,
        version=0,
        schema=None,
        shape=(0, 0),
        nbytes=0,
        owners=None,
        tags=None,
        metadata=None,
    ):
        self.name = name
        self.description = description
        self.created = created
        self.last_modified = last_modified
        self.version = version
        self.shape = shape
        self.nbytes = nbytes
        self.owners = owners or []
        self.schema = schema
        self.tags = tags or []
        self.metadata = metadata or {}

    def merge_metadata(self):
        # assert(self.schema)
        metadata = copy.deepcopy(self.metadata)
        metadata["shape"] = str(self.shape)
        metadata["nbytes"] = str(self.nbytes)
        return metadata

    def infer(self, dataset):
        # using pa.Table to get shape & size info & schema
        # start with support for the ubiquitous pandas dataframe
        if isinstance(dataset, pa.Table):
            table = dataset
        elif isinstance(dataset, pd.DataFrame):
            table = pa.Table.from_pandas(dataset)
        if table:
            self.shape = table.shape
            self.nbytes = table.nbytes
            self.schema = table.schema
        return self.schema

    def hash(self):
        # assert(self.schema)
        field_type_names = "".join(sorted([str(x.type) for x in self.schema]))
        return md5((self.name + field_type_names).encode()).hexdigest()
