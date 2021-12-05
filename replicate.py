import boto3
import botocore
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
import typer
from datahub.metadata.schema_classes import ChangeTypeClass


class SQSSource(object):

    def __init__(self, name: str):
        self._name = name
        self._sqs: botocore.client.SQS = boto3.client("sqs")
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
            VisibilityTimeout=30,
            WaitTimeSeconds=20,
        )

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        return message, receipt_handle

    def complete(self, receipt_handle):
        # Delete received message from queue
        self._sqs.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle
        )
        print(f"Received and deleted message: {receipt_handle}")


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


class Pipeline(object):

    def __init__(self, source: SQSSource, sink: DataHubSink):
        self._source = source
        self._sink = sink

    def start(self):
        while True:
            try:
                message, handle = self._source.read()
                # TODO transformed should be one of the event messages.
                transformed = message
                self._sink.write(transformed)
                self._source.complete(handle)
            except Exception as e:
                print(f"Failed to replicate Metadata change, will try again. error {e}")


def main(queue_name: str = typer.Option(..., help="Name of the queue"),
         datahub_endpoint: str = typer.Option(..., help="endpoint for datahub")):
    source = SQSSource(name=queue_name)
    sink = DataHubSink(datahub_emitter=DatahubRestEmitter(datahub_endpoint))
    p = Pipeline(source=source, sink=sink)
    p.start()


if __name__ == "__main__":
    typer.run(main)
