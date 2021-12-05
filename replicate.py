import functools
import typing

import boto3
import botocore
import typer


class SQSSource:

    def __init__(self, name: str):
        self._name = name
        self._sqs: botocore.client.SQS = boto3.client("sqs")

    def start_reading(self, callback: typing.Callable):
        queue_url = 'SQS_QUEUE_URL'

        # Receive message from SQS queue
        response = self._sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            WaitTimeSeconds=0
        )

        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']

        try:
            callback(message)

            # Delete received message from queue
            self._sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            print(f"Received and deleted message: {message}")
        except Exception as e:
            print(f"Received exception, {e}")
            raise


class DataHubSink(object):

    def __init__(self):
        pass

    def write(self, message):
        pass


def transformer(message: bytes, sink: DataHubSink):
    # transform
    transformed = message
    sink.write(transformed)


def main(queue_name: str):
    source = SQSSource(name=queue_name)
    sink = DataHubSink()
    source.start_reading(functools.partial(transformer, sink=sink))


if __name__ == "__main__":
    typer.run(main)
