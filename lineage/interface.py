from abc import ABC, abstractmethod
import typing
import pyarrow as pa
from pydantic import BaseModel
from .dataset import DatasetSchema


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


class SchemaConverter(ABC):
    """SchemaConverter

    Interface to convert from source to target schema
    """

    @abstractmethod
    def convert(self, source_schema: pa.Schema) -> typing.List[typing.Any]:
        pass


class TargetSystem(ABC):
    """TargetSystem

    TargetSystem Interface responsible for providing a ``SchemaConverter`` and
    ingesting a dataset schemas, tasks & pipelines to the target.
    """

    @abstractmethod
    def make_schema_converter(self) -> SchemaConverter:
        pass

    @abstractmethod
    def emit_dataset(self, schema: typing.Any, dataset_schema: DatasetSchema):
        pass

    @abstractmethod
    def emit_task(self, pipeline: Pipeline, task: Task):
        pass

    @abstractmethod
    def emit_pipeline(
        self,
        pipeline: Pipeline,
    ):
        pass
