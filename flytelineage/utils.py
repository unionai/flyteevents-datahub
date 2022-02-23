from typing import TypeVar
from .dataset import DatasetSchema

T = TypeVar("T")


def infer_schema(dataset: T, name: str, **kwargs):
    dataset_schema = DatasetSchema(name, **kwargs)
    source_schema = dataset_schema.infer(dataset)
    if not source_schema:
        logger.warning(
            f"dataset schema conversion for '{name}' "
            f"encountered type '{type(dataset)}' which is not supported."
        )
    return dataset_schema, source_schema
