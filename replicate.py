#!/usr/bin/env python
import base64
import json
import logging
import sys
import time
import typing

import boto3
import typer
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import ChangeTypeClass
from flyteidl.admin import event_pb2 as admin_event_pb2


def init_logger():
    logger = logging.getLogger("events")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    h = logging.StreamHandler(sys.stdout)
    h.flush = sys.stdout.flush
    logger.addHandler(h)

    return logger


logger = init_logger()


class SQSSource(object):

    def __init__(self, name: str):
        self._name = name
        self._sqs = boto3.client("sqs", region_name="us-east-2")
        response = self._sqs.get_queue_url(QueueName=name)
        self._queue_url = response['QueueUrl']

    def read(self):
        # Receive message from SQS queue
        response = self._sqs.receive_message(
            QueueUrl=self._queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=15,
            WaitTimeSeconds=10,
        )
        if "Messages" in response and len(response["Messages"]) > 0:
            message = response["Messages"][0]
            receipt_handle = message["ReceiptHandle"]
            return message, receipt_handle
        return None, None

    def complete(self, receipt_handle):
        # Delete received message from queue
        self._sqs.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle
        )
        logger.info(f"Received and deleted message: {receipt_handle}")


class DataHubSink(object):

    def __init__(self, datahub_emitter: DatahubRestEmitter):
        self._emitter = datahub_emitter

    def write(self, message):
        """
        Writer that uses example from https://github.com/linkedin/datahub/blob/master/metadata-ingestion/examples/library/lineage_emitter_mcpw_rest.py
        """
        mcw = MetadataChangeProposalWrapper(
            entityType="dataset",
            changeType=ChangeTypeClass.UPSERT,
        )
        self._emitter.emit(mcw)


def is_a_flyte_event(message: str) -> bool:
    if message is not None:
        return True
    return False


def transform_message(message: str) -> typing.Union[
    admin_event_pb2.TaskExecutionEventRequest, admin_event_pb2.NodeExecutionEventRequest, admin_event_pb2.WorkflowExecutionEventRequest]:
    message_obj = json.loads(message["Body"])
    encoded_pb = message_obj["Message"]
    data = base64.b64decode(encoded_pb)

    subj = message_obj["Subject"]
    logger.debug(f"Transforming message, subject {subj}...")

    if subj == "flyteidl.admin.TaskExecutionEventRequest":
        pb_obj = admin_event_pb2.TaskExecutionEventRequest()
        pb_obj.ParseFromString(data)
        return pb_obj

    if subj == "flyteidl.admin.NodeExecutionEventRequest":
        pb_obj = admin_event_pb2.NodeExecutionEventRequest()
        pb_obj.ParseFromString(data)
        return pb_obj

    if subj == "flyteidl.admin.WorkflowExecutionEventRequest":
        pb_obj = admin_event_pb2.WorkflowExecutionEventRequest()
        pb_obj.ParseFromString(data)
        return pb_obj

    raise ValueError(f"Could not parse message {message_obj}")


class Pipeline(object):
    def __init__(self, source: SQSSource, sink: typing.Optional[DataHubSink]):
        self._source = source
        self._sink = sink

    def start(self):
        while True:
            # Just here to give you time to hit Ctrl-C in case there are too many messages
            # At the top instead of the bottom cuz we catch exceptions at multiple layers
            time.sleep(5)
            try:
                message, handle = self._source.read()
                # TODO transformed should be one of the event messages.
                if not is_a_flyte_event(message):
                    continue
                try:
                    transformed = transform_message(message)
                except Exception as e:
                    logger.info(f"Message was not a Flyte event... skipping {message}, error {e}")
                    continue
                if self._sink:
                    self._sink.write(transformed)
                else:
                    logger.info(f"Received transformed message {transformed}")
                self._source.complete(handle)
            except Exception as e:
                logger.error(f"Failed to replicate Metadata change, will try again. error {e}")


def main(queue_name: str = typer.Option(..., help="Name of the queue"),
         datahub_endpoint: str = typer.Option(default="noendpoint", help="endpoint for datahub"),
         use_datahub: bool = typer.Option(default=False, help="endpoint for datahub")):
    source = SQSSource(name=queue_name)

    if use_datahub:
        sink = DataHubSink(datahub_emitter=DatahubRestEmitter(datahub_endpoint))
        p = Pipeline(source=source, sink=sink)
    else:
        p = Pipeline(source=source, sink=None)
    p.start()


if __name__ == "__main__":
    typer.run(main)
