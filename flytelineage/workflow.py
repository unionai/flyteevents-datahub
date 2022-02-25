import typing
import logging
from datetime import datetime

from flytelineage import error_traceback
from .dataset import DatasetSchema
from traitlets import List, Bool, Instance, default
from traitlets.config import SingletonConfigurable, Config
from .utils import infer_schema
from flyteidl.admin import event_pb2 as admin_event_pb2
from flyteidl.core import literals_pb2
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.models import literals
from flytekit.types.schema.types import FlyteSchema
import pandas as pd
import pyarrow as pa

try:
    from flytekit.exceptions.user import FlyteAssertion
except ImportError:
    # older version
    from flytekit.common.exceptions.user import FlyteAssertion
from .interface import TargetSystem, Pipeline, Task

logger = logging.getLogger(__name__)

EVENT_TYPES = typing.Union[
    admin_event_pb2.TaskExecutionEventRequest,
    admin_event_pb2.NodeExecutionEventRequest,
    admin_event_pb2.WorkflowExecutionEventRequest,
]

# TODO: reference the protobuf values directly
NodeAndTaskExecutionPhaseSucceeded = 3


class WorkflowEvents(SingletonConfigurable):
    """WorflowEvents is configured to emit lineage to one or more targets see ``TargetSystem``.
    Input is a list of flyte events which is processed to construct a pipeline of tasks and
    datasets, infering the schemas from any retrieved datasets.
    """

    emit = Bool(True, help="Emit lineage to target systems").tag(config=True)

    datasets_only = Bool(False, help="Only emit dataset lineage to target systems").tag(
        config=True
    )

    targets = List(
        Instance(TargetSystem),
        help="""List of lineage target instances.

        For instance::

            targets = [
               DataHubTarget()
            ]
        """,
    ).tag(config=True)

    @default("config")
    def _config_default(self) -> Config:
        # load application config by default
        from .app import FlyteLineage

        if FlyteLineage.initialized():
            return FlyteLineage.instance().config
        else:
            return Config()

    def get_literal_map_from_uri(
        self, remote_path: str
    ) -> typing.Optional[literals.LiteralMap]:
        ctx = FlyteContextManager.current_context()
        local_path = ctx.file_access.get_random_local_path()
        try:
            ctx.file_access.get_data(remote_path, local_path)
        except FlyteAssertion as e:
            # Getting this error using s3fs
            # Original exception: The request signature we calculated does not match the signature you provided.
            # Check your key and signing method.
            # Make sure your http proxy is not modifying the headers
            logger.warning(
                f"FlyteAssertion copying remote={remote_path} to local={local_path}, error={e}"
            )
            return None

        with open(local_path, "rb") as reader:
            lm_pb = literals_pb2.LiteralMap()
            lm_pb.ParseFromString(reader.read())
            # The output_data field is just a auto-generated python class object, convert it into our nicer model class.
            return literals.LiteralMap.from_flyte_idl(lm_pb)

    def extract_all_datasets(
        self, lm: literals.LiteralMap
    ) -> typing.List[typing.Tuple[pd.DataFrame, str]]:
        ctx = FlyteContextManager.current_context()
        datasets = []
        for k, v in lm.literals.items():
            # TODO: handle collections
            if v.scalar:
                if v.scalar.schema:
                    fs = TypeEngine.to_python_value(ctx, v, FlyteSchema)
                    reader = fs.open()
                    # TODO: handle parquet data sets split over multiple frame files?
                    parquet_file = r"/00000"
                    files = fs.local_path + parquet_file
                    df = reader._read(files)
                    self.log.info(f"read dataset rows={len(df)}")
                    datasets.append((df, v.scalar.schema.uri + parquet_file))
                # TODO: handle v.scalar.blob
        return datasets

    def get_schemas(
        self, uri: str, name: str, version: int, created: datetime
    ) -> typing.List[typing.Tuple[DatasetSchema, pa.Schema, pd.DataFrame]]:
        lm = self.get_literal_map_from_uri(uri)
        logger.debug(f"literal_map: {lm}")
        schemas = []
        if lm:
            datasets_info = self.extract_all_datasets(lm)
            if datasets_info:
                logger.info(f"Found {len(datasets_info)} datasets")
                logger.debug(datasets_info)
                for i, dataset_info in enumerate(datasets_info):
                    dataset, uri = dataset_info
                    # What about the dataset name?
                    # using task_id.name + o{arg_number} e.g. "news.workflows.covid.get.o1"
                    # would be nice if a name exists in the metadata to use that
                    metadata = dict(uri=uri)
                    schema, source_schema = infer_schema(
                        dataset,
                        f"{name}{i}",
                        version=version,
                        created=created,
                        metadata=metadata,
                    )
                    schemas.append((schema, source_schema, dataset))
        return schemas

    def get_task_version(
        self, task_event: admin_event_pb2.TaskExecutionEventRequest
    ) -> int:
        # DataHub does not support string versions only ints
        try:
            version = int(task_event.task_id.version)
        except:
            version = 0
        return version

    def fetch_input_schemas(
        self,
        task_event: admin_event_pb2.TaskExecutionEventRequest,
        node_event: admin_event_pb2.NodeExecutionEventRequest,
    ) -> typing.List[typing.Tuple[DatasetSchema, pa.Schema, pd.DataFrame]]:
        schemas = []
        try:
            name = task_event.task_id.name
            version = self.get_task_version(task_event)
            logger.debug(f"fetching input schemas for task '{name}'")
            metadata = node_event.task_node_metadata
            logger.debug(f"task node metadata: {metadata}")

            if task_event.input_uri is not None:
                schemas = self.get_schemas(
                    task_event.input_uri,
                    f"{name}.i",
                    version,
                    task_event.occurred_at.ToDatetime(),
                )

        except Exception as e:
            msg = f"Unable to fetch input schemas: error={e}, traceback={error_traceback()}"
            logger.warning(msg)
        return schemas

    def fetch_output_schemas(
        self,
        task_event: admin_event_pb2.TaskExecutionEventRequest,
        node_event: admin_event_pb2.NodeExecutionEventRequest,
    ) -> typing.List[typing.Tuple[DatasetSchema, pa.Schema, pd.DataFrame]]:
        schemas = []
        try:
            name = task_event.task_id.name
            version = self.get_task_version(task_event)
            logger.debug(f"fetching output schemas for task '{name}'")

            if task_event.HasField("output_uri"):
                schemas = self.get_schemas(
                    task_event.output_uri,
                    f"{name}.o",
                    version,
                    task_event.occurred_at.ToDatetime(),
                )

        except Exception as e:
            msg = f"Unable to fetch output schemas: error={e}, traceback={error_traceback()}"
            logger.warning(msg)
        return schemas

    @classmethod
    def get_successful_task_events(
        cls, events: typing.List[EVENT_TYPES]
    ) -> typing.List[admin_event_pb2.TaskExecutionEventRequest]:
        return [
            x
            for x in events
            if isinstance(x, admin_event_pb2.TaskExecutionEventRequest)
            and x.event.phase == NodeAndTaskExecutionPhaseSucceeded
        ]

    @classmethod
    def get_successful_node_events(
        cls, events: typing.List[EVENT_TYPES]
    ) -> typing.List[admin_event_pb2.NodeExecutionEventRequest]:
        return dict(
            [
                (x.event.id.node_id, x)
                for x in events
                if isinstance(x, admin_event_pb2.NodeExecutionEventRequest)
                and x.event.phase == NodeAndTaskExecutionPhaseSucceeded
                and not x.event.id.node_id.endswith("-node")
            ]
        )

    def create_pipeline(
        self, events: typing.List[EVENT_TYPES]
    ) -> typing.Tuple[
        Pipeline,
        typing.List[typing.Tuple[DatasetSchema, pa.Schema, pd.DataFrame]],
    ]:
        task_events = self.get_successful_task_events(events)
        node_events = self.get_successful_node_events(events)
        number_of_tasks = len(task_events)
        number_of_nodes = len(node_events)
        logger.info(
            f"create_pipeline number_of_tasks={number_of_tasks}, number_of_nodes={number_of_nodes}"
        )
        logger.debug(f"ok task events: {task_events}")
        logger.debug(f"ok node events: {node_events}")

        # assert number_of_tasks == number_of_nodes

        tasks = []
        task_id = None
        for event_request in task_events:
            event = event_request.event
            task_id = event.task_id
            log_uri = event.logs[0].uri if len(event.logs) else ""
            task = Task(
                id=event.metadata.generated_name,
                name=task_id.name,
                url=log_uri,
                version=self.get_task_version(event),
                metadata=dict(
                    input_uri_args=event.input_uri,
                    output_uri_args=event.output_uri,
                    completed=str(event.occurred_at.ToDatetime()),
                    version=task_id.version,
                ),
            )
            tasks.append((task, event))

        logger.info("wire task lineage ...")
        for i, task_info in enumerate(tasks[1:]):
            task, event = task_info
            task.upstream_task_ids = [tasks[i][0].name]

        all_schemas = []
        logger.info("wire any output dataset schemas ...")
        for i, task_info in enumerate(tasks):
            task, task_event = task_info
            node_id = f"n{i}"
            node_event = node_events[node_id]
            if i == 0:
                schemas = self.fetch_input_schemas(task_event, node_event.event)
                if schemas:
                    # wire dataset names to task inputs
                    task.inputs = [x[0].name for x in schemas]
                    logger.info(f"found input schemas: {task.inputs}")
                    all_schemas.extend(schemas)

            schemas = self.fetch_output_schemas(task_event, node_event.event)
            if schemas:
                # wire dataset names to task outputs
                task.outputs = [x[0].name for x in schemas]
                logger.info(f"found schemas: {task.outputs}")
                next_index = i + 1
                if task.outputs and next_index < number_of_tasks:
                    tasks[next_index][0].inputs = task.outputs
                all_schemas.extend(schemas)

        name = task_id.name.rpartition(".")[0]
        _id = tasks[0][0].id.split("-")[0]
        name = f"{task_id.domain}.{task_id.project}.{name}"
        pipeline = Pipeline(
            name=name, id=_id, owners=[], tags=[], tasks=[x[0] for x in tasks]
        )
        logger.debug(f"pipeline: {pipeline}")
        return pipeline, all_schemas

    def ingest(self, events: typing.List[EVENT_TYPES]):
        try:
            logger.info("ingest")
            pipeline, schemas = self.create_pipeline(events)
            logger.debug(
                f"pipeline '{pipeline.name}:{pipeline.id}'  has '{pipeline.number_of_tasks()}' "
                f"tasks: {pipeline.task_names()} with schemas={ list([x[0].name for x in schemas]) }"
            )
            if self.emit:
                for target in self.targets:
                    try:
                        target.ingest(pipeline, schemas)
                    except Exception as e:
                        msg = (
                            f"Unable to ingest pipeline '{pipeline.name}' with target '{target}', "
                            f"error={e}, traceback={error_traceback()}"
                        )
                        logger.warning(msg)
        except Exception as e:
            msg = f"Unable to ingest pipeline, error={e}, traceback={error_traceback()}"
            logger.warning(msg)
