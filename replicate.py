import boto3
import botocore
import typer


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

    def __init__(self):
        pass

    def write(self, message):
        pass


class Pipeline(object):

    def __init__(self, source: SQSSource, sink: DataHubSink):
        self._source = source
        self._sink = sink

    def start(self):
        while True:
            try:
                message, handle = self._source.read()
                # transform
                transformed = message
                self._sink.write(transformed)
                self._source.complete(handle)
            except Exception as e:
                pass


def main(queue_name: typer.Option("", help="Name of the queue")):
    source = SQSSource(name=queue_name)
    sink = DataHubSink()
    p = Pipeline(source=source, sink=sink)
    p.start()


if __name__ == "__main__":
    typer.run(main)
