from typing import Union
import logging
from .dataset import DatasetSchema
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)


def infer_schema(dataset: Union[pa.Table, pd.DataFrame], name: str, **kwargs):
    dataset_schema = DatasetSchema(name, **kwargs)
    source_schema = dataset_schema.infer(dataset)
    if not source_schema:
        logger.warning(
            f"dataset schema conversion for '{name}' "
            f"encountered type '{type(dataset)}' which is not supported."
        )
    return dataset_schema, source_schema
