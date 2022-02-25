import base64
import json
import logging
import sys
import typing
from concurrent.futures import ThreadPoolExecutor

from botocore.session import get_session, Session
from flyteidl.admin import event_pb2 as admin_event_pb2
from retry import retry
from traitlets import Any, Dict, Instance, Unicode, default
from traitlets.config import Application

from flytelineage import __version__ as lineage_version
from flytelineage import error_traceback

from .workflow import WorkflowEvents, EVENT_TYPES

logger = logging.getLogger(__name__)

# TODO: reference the protobuf values directly
WorkflowExecutionPhaseSucceeding = 3
WorkflowExecutionPhaseSucceeded = 4


class SQSSource(object):
    """SQSSource

    AWS SQS Source

    ``name`` The SQS queue URL or queue name can be supplied. If queue name is used the queue url
    will be looked up.

    ``region_name`` AWS region to use. Defaults to 'us-east-1'.

    ``session`` The botocore.session.Session. If not supplied a default session is used via get_session().
    """

    def __init__(
        self, name: str, region_name: str = "us-east-1", session: Session = None
    ):
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

    def read(self) -> typing.Tuple[typing.Dict, str]:
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
        result = self._sqs.delete_message(
            QueueUrl=self._queue_url, ReceiptHandle=receipt_handle
        )
        logger.debug(
            f"Deleted message, http status: {result['ResponseMetadata']['HTTPStatusCode']}"
        )
        return result


def event_comparison_key(event_request: EVENT_TYPES):
    return event_request.event.occurred_at.ToNanoseconds()


class FlyteLineage(Application):
    """FlyteLineage application class

    The application is initilized with traitlets via default settings, config file or cmd line args.

    The app reads flyte events from an SQS queue. The workflow events are gathered and then ingested via
    ``WorkflowEvents.ingest`` in a thread pool.
    """

    name = "flytelineage"
    version = lineage_version
    description = """Flyte data lineage"""

    config_file = Unicode("flytelineage_config.py", help="The config file to load").tag(
        config=True
    )

    @default("log_level")
    def _log_level_default(self):
        return logging.INFO

    sqs_queue = Unicode(
        help="sqs queue name or url",
    ).tag(config=True)

    aws_region = Unicode(
        "us-east-1",
        help="aws region",
    ).tag(config=True)

    sqs_source = Instance(SQSSource)

    @default("sqs_source")
    def _sqs_source_default(self) -> SQSSource:
        return SQSSource(name=self.sqs_queue, region_name=self.aws_region)

    executor = Any()

    def _executor_default(self) -> ThreadPoolExecutor:
        return ThreadPoolExecutor(thread_name_prefix="workflow")

    _db = Dict({})

    def initialize(self, *args, **kwargs):
        super().initialize(*args, **kwargs)
        self.load_config_file(self.config_file)
        self.init_logging()
        self.log.info(f"Initialized with {self.config}")
        if not self.config.WorkflowEvents.emit:
            logger.warning("Workflow event ingestion is disabled!")

    def init_logging(self):
        # disable botocore debug
        logging.getLogger("botocore").setLevel(max(self.log_level, logging.INFO))
        logger = logging.getLogger()
        logger.propagate = True
        logger.parent = self.log
        logger.setLevel(self.log_level)

    def is_a_flyte_event(self, message: str) -> bool:
        return message is not None

    def transform_message(
        self,
        message: typing.Dict,
    ) -> EVENT_TYPES:
        message_obj = json.loads(message["Body"])
        encoded_pb = message_obj["Message"]
        data = base64.b64decode(encoded_pb)

        subj = message_obj["Subject"]
        self.log.debug(f"Transforming message, subject {subj}...")
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

    def get_workflow_id(self, event_request: EVENT_TYPES) -> str:
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

    def add_event(self, event_request: EVENT_TYPES) -> str:
        workflow_id = self.get_workflow_id(event_request)
        if workflow_id not in self._db:
            self.log.info(f"receiving events for workflow: {workflow_id}")
            self._db[workflow_id] = []
        self._db[workflow_id].append(event_request)
        return workflow_id

    def has_successful_workflow(self, events: EVENT_TYPES) -> bool:
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

    def is_successful_new_workflow(self, events: EVENT_TYPES) -> bool:
        completed = False
        if self.has_successful_workflow(events):
            # policy is to only capture workflows when all tasks have been executed
            # this excludes any workflows with tasks outputs retrieved from the cache as
            # these don't run the task so there is no corresponding task event for the node
            completed = len(WorkflowEvents.get_successful_task_events(events)) == len(
                WorkflowEvents.get_successful_node_events(events).keys()
            )
            self.log.info(
                f"found a completed workflow, do we have the same number of successful"
                f" task and node events? {'yes' if completed else 'no!'}"
            )
        return completed

    @retry(delay=1, tries=10, backoff=1.2, logger=logging)
    def process_events(self, workflow: WorkflowEvents):
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
                    # events should be >= 8 as 8 events are produced per task
                    if len(events) > 7:
                        logger.info(f"emit_pipeline for workflow: {workflow_id}")
                        future = self.executor.submit(workflow.ingest, events)

                        def finished(result):
                            logger.info(f"workflow '{workflow_id}' ingestion finished")

                        future.add_done_callback(finished)
            except Exception as e:
                msg = f"error: exception={e}, traceback={error_traceback()}"
                logger.error(msg)
            finally:
                self.sqs_source.complete(handle)

    @classmethod
    def launch_instance(cls, argv=None):
        try:
            self = cls.instance()
            self.initialize(argv)
            self.process_events(workflow=WorkflowEvents())
        except Exception as e:
            msg = f"error: exception={e}, traceback={error_traceback()}"
            logger.error(msg)
            sys.exit(-1)


main = FlyteLineage.launch_instance

if __name__ == "__main__":
    main()
