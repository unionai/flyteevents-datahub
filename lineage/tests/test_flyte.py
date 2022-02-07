import pytest
import moto
import boto3


@pytest.fixture(scope="module")
def moto_sqs():
    with moto.mock_sqs():
        sqs = boto3.resource("sqs", region_name="us-east-1")
        q = sqs.create_queue(QueueName="foo")
        print(q)
        yield q


def test_sqssource_ctor(moto_sqs):
    from lineage.flyte import SQSSource

    sqs_source = SQSSource("foo")
    assert isinstance(sqs_source, SQSSource)


def test_sqssource_ctor_q_uri(moto_sqs):
    from lineage.flyte import SQSSource

    sqs_source = SQSSource(moto_sqs.url)
    assert isinstance(sqs_source, SQSSource)


def test_sqssource_read(moto_sqs):
    from lineage.flyte import SQSSource

    sqs_source = SQSSource("foo")
    result = sqs_source.read()
    assert result == (None, None)


def test_sqssource_read_and_complete(moto_sqs):
    from lineage.flyte import SQSSource

    sqs_source = SQSSource("foo")
    moto_sqs.send_message(MessageBody="Hello World")
    result = sqs_source.read()
    assert result[0]["Body"] == "Hello World"
    result = sqs_source.complete(result[0]["ReceiptHandle"])
    assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_flytelineage_ctor():
    from lineage.flyte import FlyteLineage

    flyte = FlyteLineage.instance()
    assert flyte.config is not None
