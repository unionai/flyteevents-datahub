import pytest
import moto
import boto3


@pytest.fixture(scope="function")
def moto_s3():
    with moto.mock_s3():
        s3 = boto3.resource("s3", region_name="us-east-1")
        s3.create_bucket(
            Bucket="bucket",
        )
        yield s3


@pytest.fixture(scope="module")
def moto_glue():
    import os

    with moto.mock_glue():
        region_name = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = region_name
        glue = boto3.client("glue", region_name=region_name)
        yield glue


def test_glue(moto_glue, moto_s3):
    from flytelineage.glue import GlueCatalogTarget

    target = GlueCatalogTarget(bucket_path="bucket/prefix", kms_key_arn="bogus")

    from flytelineage.interface import Pipeline

    pipeline = Pipeline(
        id="1",
        name="a.b.c",
    )
    from flytelineage.dataset import DatasetSchema
    import numpy as np
    import pandas as pd

    ds = DatasetSchema("foo")
    a = np.array([[5, "hello", True], [2, "goodbye", False]])
    df = pd.DataFrame(a, columns=list("ABC"))
    schema = ds.infer(df)
    dataset = (ds, schema, df)

    result = target.ingest(pipeline, [dataset])
    assert len(result) == 1
    assert result[0].get("paths")[0].startswith("s3://bucket/prefix/flyte_a_b/foo/")


def test_glue_with_db(moto_glue, moto_s3):
    database_name = "mydb"

    import awswrangler as wr

    wr.catalog.create_database(name=database_name)
    from flytelineage.glue import GlueCatalogTarget

    target = GlueCatalogTarget(
        bucket_path="bucket/prefix", kms_key_arn="bogus", db_name=database_name
    )

    from flytelineage.interface import Pipeline

    pipeline = Pipeline(
        id="1",
        name="a.b.c",
    )
    from flytelineage.dataset import DatasetSchema
    import numpy as np
    import pandas as pd

    ds = DatasetSchema("foo")
    a = np.array([[5, "hello", True], [2, "goodbye", False]])
    df = pd.DataFrame(a, columns=list("ABC"))
    schema = ds.infer(df)
    dataset = (ds, schema, df)

    result = target.ingest(pipeline, [dataset])
    assert len(result) == 1
    assert result[0].get("paths")[0].startswith("s3://bucket/prefix/mydb/foo")


def test_glue_error(moto_glue, moto_s3):
    from flytelineage.glue import GlueCatalogTarget

    target = GlueCatalogTarget(bucket_path="bucket/prefix", kms_key_arn="bogus")

    from flytelineage.interface import Pipeline

    pipeline = Pipeline(
        id="1",
        name="a.b.c",
    )
    from flytelineage.dataset import DatasetSchema
    import numpy as np
    import pandas as pd

    ds = DatasetSchema("foo")
    a = np.array([[5, "hello", True], [2, "goodbye", False]])
    df = pd.DataFrame(a)
    schema = ds.infer(df)
    dataset = (ds, schema, df)
    result = target.ingest(pipeline, [dataset])
    assert len(result) == 0
