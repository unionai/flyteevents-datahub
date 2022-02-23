from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any
import pyarrow as pa
import pandas as pd
from .dataset import DatasetSchema
from pydantic import BaseModel


class SchemaConverter(ABC):
    """SchemaConverter

    Interface to convert from our source schema (pyarrow.Schema) to a target schema
    """

    @abstractmethod
    def convert(self, source_schema: pa.Schema) -> Any:
        pass


class Task(BaseModel):
    """Task represents a unit of processing in a workflow such as the task identity
    and any input or output datasets.

    ``id`` task id
    ``name`` name of the task
    ``description`` task description
    ``version`` task version, defaults to 0
    ``inputs`` The task input dataset names
    ``outputs`` The task output dataset names
    ``tags`` Any tag names associated with the task
    ``owners`` ownership names for the task
    ``metadata`` dictionary to store any other task information
    ``url`` url reference location of the task
    """

    id: str = ""
    name: str
    description: str = ""
    version: int = 0
    inputs: List[str] = []
    outputs: List[str] = []
    upstream_task_ids: List[str] = []
    tags: List[str] = []
    owners: List[str] = []
    metadata: Dict = {}
    url: str = ""


class Pipeline(BaseModel):
    """Pipeline represents all the entities involved in a workflow."""

    id: str = ""
    name: str
    description: str = ""
    tasks: List[Task] = []
    tags: List[str] = []
    owners: List[str] = []
    metadata: Dict = {}
    url: str = ""

    def number_of_tasks(self):
        return len(self.tasks)

    def task_names(self):
        return list([x.name for x in self.tasks])


class TargetSystem(ABC):
    """TargetSystem is an endpoint for lineage information contained in a pipeline.
    Example targets include AWS Glue and DataHub.
    """

    @abstractmethod
    def ingest(
        self,
        pipeline: Pipeline,
        datasets: List[Tuple[DatasetSchema, pa.Schema, pd.DataFrame]],
    ) -> Any:
        pass
