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

logger = logging.getLogger()


# TODO: reference the protobuf values directly
NodeAndTaskExecutionPhaseSucceeded = 3
WorkflowExecutionPhaseSucceeding = 3
WorkflowExecutionPhaseSucceeded = 4
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
        response = self._sqs.get_queue_url(QueueName=name)
        self._queue_url = response["QueueUrl"]

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
        logger.info(f"Received and deleted message: {receipt_handle}")


class Workflow(object):
    def __init__(self, target: TargetSystem, emit=True):
        self.target = target
        self.emit = emit

    def get_literal_map_from_uri(
        self, remote_path: str
    ) -> typing.Optional[literals.LiteralMap]:
        ctx = FlyteContextManager.current_context()
        local_path = ctx.file_access.get_random_local_path()
        try:
            ctx.file_access.get_data(remote_path, local_path)
        except FlyteAssertion:
            return None

        with open(local_path, "rb") as reader:
            lm_pb = literals_pb2.LiteralMap()
            lm_pb.ParseFromString(reader.read())
            # The output_data field is just a auto-generated python class object, convert it into our nicer model class.
            return literals.LiteralMap.from_flyte_idl(lm_pb)

    def extract_all_schemas(self, lm: literals.LiteralMap) -> typing.List[FlyteSchema]:
        ctx = FlyteContextManager.current_context()
        r = []
        for k, v in lm.literals.items():
            if v.scalar and v.scalar.schema:
                fs = TypeEngine.to_python_value(ctx, v, FlyteSchema)
                reader = fs.open()
                # TODO: handle parquet data sets split over multiple frame files
                files = fs.local_path + r"/00000"
                df = reader._read(files)
                logger.info(f"read dataset rows={len(df)}")
                r.append(df)
        return r

    def fetch_schemas(self, event: event_pb2.NodeExecutionEvent):
        # an example node execution event that is populating the cache
        """
        event {
        id {
            node_id: "n1"
            execution_id {
            project: "poc"
            domain: "development"
            name: "faffp5fg4t"
            }
        }
        phase: SUCCEEDED
        occurred_at {
            seconds: 1641493633
            nanos: 928646612
        }
        input_uri: "s3://app-id-103020-dep-id-103021-uu-id-5qqzivkh5yaj/metadata/propeller/poc-development-faffp5fg4t/n1/data/inputs.pb"
        output_uri: "s3://app-id-103020-dep-id-103021-uu-id-5qqzivkh5yaj/metadata/propeller/poc-development-faffp5fg4t/n1/data/0/outputs.pb"
        spec_node_id: "n1"
        node_name: "flytekit.core.python_function_task.news.workflows.covid.filter_data"
        task_node_metadata {
            cache_status: CACHE_POPULATED
            catalog_key {
            dataset_id {
                resource_type: DATASET
                project: "poc"
                domain: "development"
                name: "flyte_task-news.workflows.covid.filter_data"
                version: "1.0-Ii9-ktJ0-c5per3_7"
            }
            artifact_tag {
                artifact_id: "6e466e7f-70f3-434a-b143-9bdfbe70326c"
                name: "flyte_cached-Av7uhW5T1i1vNtEXNzl9g-LGkTLhRRQ7-rO50b9yaTI"
            }
            }
        }
        """
        schemas = []
        try:
            metadata = event.task_node_metadata
            name = metadata.catalog_key.dataset_id.name
            version = metadata.catalog_key.dataset_id.version
            artifact_id = metadata.catalog_key.artifact_tag.artifact_id
            if event.HasField("output_uri"):
                lm = self.get_literal_map_from_uri(event.output_uri)
                if lm:
                    datasets = self.extract_all_schemas(lm)
                    if datasets:
                        logger.info(f"Found datasets from output_uri")
                        logger.debug(datasets)
                        for i, dataset in enumerate(datasets):
                            # What about the dataset name?
                            # using dataset_id.name + + o{node}
                            # e.g. "flyte_task-news.workflows.covid.filter_data.o1"
                            # TODO: if name exists in the metadata, use that. Will need to preserve
                            # metadata across io pandas -> parquet -> pandas
                            # DataHub does not support string versions
                            metadata = dict(version=version)
                            schema, source_schema = infer_schema(
                                dataset,
                                f"{name}.o{i}",
                                created=event.occurred_at.ToDatetime(),
                                metadata=metadata,
                            )
                            schemas.append((schema, source_schema))

        except Exception as e:
            msg = f"Unable to fetch data sets: error={e}, traceback={error_traceback()}"
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

    def create_pipeline(self, events: typing.List):
        task_events = self.get_successful_task_events(events)
        node_events = self.get_successful_node_events(events)
        number_of_tasks = len(task_events)
        number_of_nodes = len(node_events)
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
        logger.info("wire any output dataset cached schemas ...")
        for i, task_info in enumerate(tasks):
            task, task_event = task_info
            node_id = f"n{i}"
            node_event = node_events[node_id]
            if (
                hasattr(node_event.event, "task_node_metadata")
                and node_event.event.task_node_metadata.cache_status == CACHE_POPULATED
            ):
                schemas = self.fetch_schemas(node_event.event)
                if schemas:
                    # wire dataset names to task outputs
                    task.outputs = [x[0].name for x in schemas]
                    logger.info(f"found schemas: {task.outputs}")
                    next_index = i + 1
                    if task.outputs and next_index <= number_of_tasks:
                        tasks[next_index][0].inputs = task.outputs
                    try:
                        for schema_info in schemas:
                            # emit dataset as its own entity
                            self.emit_dataset(schema_info)
                    except Exception as e:
                        msg = f"Unable to emit data sets: error={e}, traceback={error_traceback()}"
                        logger.warning(msg)

        name = task_id.name.rpartition(".")[0]
        _id = tasks[0][0].id.split("-")[0]
        name = f"{task_id.domain}.{task_id.project}.{name}"
        pipeline = Pipeline(
            name=name, id=_id, owners=[], tags=[], tasks=[x[0] for x in tasks]
        )
        logger.debug(f"pipeline: {pipeline}")
        return pipeline

    def emit_dataset(self, schema_info):
        dataset_schema, source_schema = schema_info
        target = self.target
        schema_converter = target.make_schema_converter()
        datahub_schema = schema_converter.convert(source_schema)
        if self.emit:
            target.emit_dataset(datahub_schema, dataset_schema)

    def emit_pipeline(self, events: typing.List):
        pipeline = self.create_pipeline(events)
        if pipeline and self.emit:
            self.target.emit_pipeline(pipeline)


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
            completed = len(Workflow.get_successful_task_events(events)) == len(
                Workflow.get_successful_node_events(events).keys()
            )
        return completed

    @retry(delay=1, tries=10, backoff=1.2, logger=logging)
    def start(self, workflow: Workflow):
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
                    # events shoube be >= 8 as 8 events are produced per task
                    if len(events) > 7:
                        logger.info(f"emit_pipeline for workflow: {workflow_id}")
                        workflow.emit_pipeline(events)
                        logger.info(
                            f"workflow '{workflow_id}' emitted to target lineage system"
                        )
            except Exception as e:
                msg = f"error: exception={e}, traceback={error_traceback()}"
                logger.error(msg)
            finally:
                self.sqs_source.complete(handle)
