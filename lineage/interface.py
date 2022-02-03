from abc import ABC, abstractmethod
import typing
import pyarrow as pa
from pydantic import BaseModel
from .dataset import DatasetSchema, pd


class SchemaConverter(ABC):
    """SchemaConverter
    Interface to convert from source to target schema
    """

    @abstractmethod
    def convert(self, source_schema: pa.Schema) -> typing.List[typing.Any]:
        pass


class Task(BaseModel):
    id: str = ""
    name: str
    description: str = ""
    inputs: typing.List[str] = []
    outputs: typing.List[str] = []
    upstream_task_ids: typing.List[str] = []
    tags: typing.List[str] = []
    owners: typing.List[str] = []
    metadata: typing.Dict = {}
    url: str = ""


class Pipeline(BaseModel):
    id: str = ""
    name: str
    description: str = ""
    tasks: typing.List[Task] = []
    tags: typing.List[str] = []
    owners: typing.List[str] = []
    metadata: typing.Dict = {}
    url: str = ""

    def number_of_tasks(self):
        return len(self.tasks)

    def task_names(self):
        return list([x.name for x in self.tasks])


class TargetSystem(ABC):
    """TargetSystem"""

    @abstractmethod
    def ingest(self, pipeline, datasets):
        pass
