import logging
import typing
import time
import base64
import json
from datetime import datetime, timezone
import configparser
import os


from .interface import TargetSystem, Pipeline, Task
from lineage import error_traceback
from .dataset import DatasetSchema
from .utils import infer_schema
from flyteidl.admin import event_pb2 as admin_event_pb2
from flyteidl.core import literals_pb2, execution_pb2
from flyteidl.event import event_pb2
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.models import literals
from flytekit.types.schema.types import FlyteSchema
from flytekit.common.exceptions.user import FlyteAssertion
from google.protobuf.timestamp_pb2 import Timestamp
import boto3
from botocore.session import get_session, Session
from retry import retry

logger = logging.getLogger(__name__)


# TODO: reference the protobuf values directly
NodeAndTaskExecutionPhaseSucceeded = 3
WorkflowExecutionPhaseSucceeding = 3
WorkflowExecutionPhaseSucceeded = 4
CACHE_DISABLED = 0
CACHE_POPULATED = 3
DATASET = 4

EVENT_TYPES = typing.Union[
    admin_event_pb2.TaskExecutionEventRequest,
    admin_event_pb2.NodeExecutionEventRequest,
    admin_event_pb2.WorkflowExecutionEventRequest,
]


class SQSSource(object):
    def __init__(self, name: str, region_name="us-east-1", session=None):
        self._name = name
        session = session or get_session()
        self._sqs = session.create_client("sqs", region_name=region_name)
        if "amazonaws.com" in name:
            self._queue_url = name
        else:
            logger.info(f"lookup the queue url with name={name}")
            response = self._sqs.get_queue_url(QueueName=name)
            self._queue_url = response["QueueUrl"]
        logger.info(f"will be reading from queue: {self._queue_url}")

    def read(self):
        # Receive message from SQS queue
        response = self._sqs.receive_message(
            QueueUrl=self._queue_url,
            AttributeNames=["SentTimestamp"],
            MaxNumberOfMessages=1,
            MessageAttributeNames=["All"],
            VisibilityTimeout=15,
            WaitTimeSeconds=10,
        )
        if "Messages" in response and len(response["Messages"]) > 0:
            message = response["Messages"][0]
            receipt_handle = message["ReceiptHandle"]
            return message, receipt_handle
        return None, None

    def complete(self, receipt_handle):
        self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=receipt_handle)
        logger.debug(f"Received and deleted message: {receipt_handle}")


class WorkflowEvents(object):
    def __init__(self, targets, emit=True, datasets_only=False):
        self.targets = targets
        self.emit = emit
        self.datasets_only = datasets_only

    def get_literal_map_from_uri(
        self, remote_path: str
    ) -> typing.Optional[literals.LiteralMap]:
        ctx = FlyteContextManager.current_context()
        local_path = ctx.file_access.get_random_local_path()
        try:
            ctx.file_access.get_data(remote_path, local_path)
        except FlyteAssertion as e:
            # Getting this error using s3fs
            # Original exception: The request signature we calculated does not match the signature you provided. Check your key and signing method.
            logger.warning(
                f"FlyteAssertion copying remote={remote_path} to local={local_path}, error={e}"
            )
            return None

        with open(local_path, "rb") as reader:
            lm_pb = literals_pb2.LiteralMap()
            lm_pb.ParseFromString(reader.read())
            # The output_data field is just a auto-generated python class object, convert it into our nicer model class.
            return literals.LiteralMap.from_flyte_idl(lm_pb)

    def extract_all_datasets(self, lm: literals.LiteralMap):
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
                    logger.info(f"read dataset rows={len(df)}")
                    datasets.append((df, v.scalar.schema.uri + parquet_file))
                # TODO: handle v.scalar.blob
        return datasets

    def get_schemas(self, uri, name, version, created):
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
                    # using task_id.name + + o{arg_number} e.g. "news.workflows.covid.get.o1"
                    # TODO: if name exists in the metadata, use that. Will need to preserve
                    # metadata across io pandas -> parquet -> pandas
                    metadata = dict(uri=uri)
                    schema, source_schema = infer_schema(
                        dataset,
                        f"{name}{i}",
                        version=version,
                        created=created,
                        metadata=metadata,
                    )
                    schemas.append((schema, source_schema))
        return schemas

    def get_task_version(self, task_event):
        # DataHub does not support string versions only ints
        try:
            version = int(task_event.task_id.version)
        except:
            version = 0
        return version

    def fetch_input_schemas(self, task_event, node_event):
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

    def fetch_output_schemas(self, task_event, node_event):

        schemas = []
        try:
            name = task_event.task_id.name
            version = self.get_task_version(task_event)
            logger.debug(f"fetching output schemas for task '{name}'")
            metadata = node_event.task_node_metadata
            if not metadata.cache_status in (CACHE_DISABLED, CACHE_POPULATED):
                logger.debug(f"skip datasets being retrieved from the cache")
                return schemas

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
    def get_successful_task_events(cls, events):
        return [
            x
            for x in events
            if isinstance(x, admin_event_pb2.TaskExecutionEventRequest)
            and x.event.phase == NodeAndTaskExecutionPhaseSucceeded
        ]

    @classmethod
    def get_successful_node_events(cls, events):
        return dict(
            [
                (x.event.id.node_id, x)
                for x in events
                if isinstance(x, admin_event_pb2.NodeExecutionEventRequest)
                and x.event.phase == NodeAndTaskExecutionPhaseSucceeded
                and not x.event.id.node_id.endswith("-node")
            ]
        )

    def create_pipeline(self, events):
        task_events = self.get_successful_task_events(events)
        node_events = self.get_successful_node_events(events)
        number_of_tasks = len(task_events)
        number_of_nodes = len(node_events)
        logger.debug(
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
                if task.outputs and next_index <= number_of_tasks:
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

    def ingest(self, events):
        pipeline, schemas = self.create_pipeline(events)
        logger.debug(
            f"pipeline '{pipeline.name}:{pipeline.id}'  has '{pipeline.number_of_tasks()}' tasks: {pipeline.task_names()} with schemas={ list([x[0].name for x in schemas]) }"
        )
        if self.emit:
            for target in self.targets:
                for schema_info in schemas:
                    dataset_schema, source_schema = schema_info
                    try:
                        # emit dataset as its own entity
                        schema_converter = target.make_schema_converter()
                        target_schema = schema_converter.convert(source_schema)
                        logger.info(f"emitting dataset {dataset_schema.name}")
                        target.emit_dataset(target_schema, dataset_schema)
                        logger.info("emitted dataset")
                    except Exception as e:
                        msg = f"Unable to emit data set '{schema_info[0].name}', error={e}, traceback={error_traceback()}"
                        logger.warning(msg)
            if not self.datasets_only:
                for target in self.targets:
                    logger.info(f"emit_pipeline to target '{type(target)}'")
                    target.emit_pipeline(pipeline)


def event_comparison_key(event_request: EVENT_TYPES):
    return event_request.event.occurred_at.ToNanoseconds()


class EventProcesser(object):
    def __init__(self, sqs_source: SQSSource):
        self.sqs_source = sqs_source
        self._db = {}

    def is_a_flyte_event(self, message: str) -> bool:
        return message is not None

    def transform_message(
        self,
        message: str,
    ) -> EVENT_TYPES:
        message_obj = json.loads(message["Body"])
        encoded_pb = message_obj["Message"]
        data = base64.b64decode(encoded_pb)

        subj = message_obj["Subject"]
        logger.debug(f"Transforming message, subject {subj}...")
        pb_obj = None

        if subj == "flyteidl.admin.TaskExecutionEventRequest":
            pb_obj = admin_event_pb2.TaskExecutionEventRequest()
            pb_obj.ParseFromString(data)
        elif subj == "flyteidl.admin.NodeExecutionEventRequest":
            pb_obj = admin_event_pb2.NodeExecutionEventRequest()
            pb_obj.ParseFromString(data)
        elif subj == "flyteidl.admin.WorkflowExecutionEventRequest":
            pb_obj = admin_event_pb2.WorkflowExecutionEventRequest()
            pb_obj.ParseFromString(data)
        else:
            msg = f"Received unexpected event, subject={subj}"
            raise ValueError(msg)
        return pb_obj

    def get_workflow_id(self, event_request: EVENT_TYPES):
        event = event_request.event
        if hasattr(event, "parent_node_execution_id"):
            id_attr = "parent_node_execution_id"
            event = getattr(event, id_attr)
        elif hasattr(event, "id"):
            id_attr = "id"
            event = getattr(event, id_attr)
        if not hasattr(event, "execution_id"):
            raise ValueError(f"Unexpected event: {event}")
        _id = event.execution_id
        return f"{_id.project}-{_id.domain}-{_id.name}"

    def add_event(self, event_request: EVENT_TYPES):
        workflow_id = self.get_workflow_id(event_request)
        if workflow_id not in self._db:
            logger.info(f"receiving events for workflow: {workflow_id}")
            self._db[workflow_id] = []
        self._db[workflow_id].append(event_request)
        return workflow_id

    def has_successful_workflow(self, events):
        # I have seen workflows without a workflow succeeded event?
        return (
            len(
                [
                    x
                    for x in events
                    if isinstance(x, admin_event_pb2.WorkflowExecutionEventRequest)
                    and x.event.phase
                    in (
                        WorkflowExecutionPhaseSucceeded,
                        WorkflowExecutionPhaseSucceeding,
                    )
                    and "end-node" in x.event.output_uri
                ]
            )
            == 1
        )

    def is_successful_new_workflow(self, events):
        completed = False
        # assuming here there is always an output uri even for void returns
        if self.has_successful_workflow(events):
            # current policy is to only capture workflows when all tasks have been executed
            # this excludes any workflows with tasks outputs retrieved from the cache
            # these don't run the task so there is no corresponding task event for the node
            completed = len(WorkflowEvents.get_successful_task_events(events)) == len(
                WorkflowEvents.get_successful_node_events(events).keys()
            )
        return completed

    @retry(delay=1, tries=10, backoff=1.2, logger=logging)
    def start(self, workflow):
        while True:
            message, handle = self.sqs_source.read()
            if not self.is_a_flyte_event(message):
                continue
            try:
                event_request = self.transform_message(message)
                logger.debug(f"transformed: {event_request}")
                workflow_id = self.add_event(event_request)
                # wait until all workflow events have arrived before ingesting
                if self.is_successful_new_workflow(self._db[workflow_id]):
                    # events might not arrive in order
                    events = sorted(self._db.pop(workflow_id), key=event_comparison_key)
                    logger.info(
                        f"processing {len(events)} events for workflow {workflow_id}"
                    )
                    # events shoube be >= 8 as 8 events are produced per task
                    if len(events) > 7:
                        logger.info(f"emit_pipeline for workflow: {workflow_id}")
                        workflow.ingest(events)
                        logger.info(f"workflow '{workflow_id}' ingestion finished")
            except Exception as e:
                msg = f"error: exception={e}, traceback={error_traceback()}"
                logger.error(msg)
            finally:
                self.sqs_source.complete(handle)
