def make_converter():
    from lineage.datahub import DataHubSchemaConverter

    return DataHubSchemaConverter()


def test_convert():
    import pyarrow as pa

    fields = [
        pa.field("bool", pa.bool_()),
        pa.field("int8", pa.int8()),
        pa.field("int16", pa.int16()),
        pa.field("int32", pa.int32()),
        pa.field("int64", pa.int64()),
        pa.field("uint8", pa.uint8()),
        pa.field("uint16", pa.uint16()),
        pa.field("uint32", pa.uint32()),
        pa.field("float16", pa.float16()),
        pa.field("float32", pa.float32()),
        pa.field("float64", pa.float64()),
        pa.field("U", pa.string()),
        pa.field("S", pa.binary()),
        pa.field("datetime64[s]", pa.timestamp("s")),
        pa.field("datetime64[ms]", pa.timestamp("ms")),
        pa.field("datetime64[us]", pa.timestamp("us")),
        pa.field("datetime64[ns]", pa.timestamp("ns")),
        pa.field("timedelta64[s]", pa.duration("s")),
        pa.field("timedelta64[ms]", pa.duration("ms")),
        pa.field("timedelta64[us]", pa.duration("us")),
        pa.field("timedelta64[ns]", pa.duration("ns")),
    ]
    s = pa.schema(fields)
    converter = make_converter()
    result = converter.convert(s)
    assert len(result) == len(fields)


def make_target(**kwargs):
    from lineage.datahub import DataHubTarget

    return DataHubTarget(server="https://a.com", test_connection=False, **kwargs)


def test_make_pipeline_snaphot():
    target = make_target()
    from lineage.interface import Pipeline, Task

    pipeline = Pipeline(
        id="1",
        name="p1",
        description="A wonderful job",
        url="https://foo.com",
        metadata=dict(a="x", b=2),
        tags=["sea", "beach"],
        owners=["xyz"],
    )
    snapshot = target.make_pipeline_snapshot(pipeline)
    assert snapshot.urn == "urn:li:dataFlow:(flyte,p1,DEV)"
    assert len(snapshot.aspects) == 3
    dataflow_aspect = snapshot.aspects[0]
    assert dataflow_aspect.name == "p1"
    assert dataflow_aspect.description == "A wonderful job"
    assert dataflow_aspect.externalUrl == "https://foo.com"
    assert dataflow_aspect.customProperties == dict(a="x", b=2)
    owners_aspect = snapshot.aspects[1]
    assert (
        str(owners_aspect)
        == "OwnershipClass({'owners': [OwnerClass({'owner': 'urn:li:corpuser:xyz', 'type': 'DATAOWNER', 'source': None})], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None})})"
    )
    tag_aspect = snapshot.aspects[2]
    assert (
        str(tag_aspect)
        == "GlobalTagsClass({'tags': [TagAssociationClass({'tag': 'urn:li:tag:sea'}), TagAssociationClass({'tag': 'urn:li:tag:beach'})]})"
    )


def test_make_dataset_snaphot():
    target = make_target()
    from lineage.dataset import DatasetSchema
    import pandas as pd
    import numpy as np

    df = pd.DataFrame(
        {
            "A": 1.0,
            "B": pd.Timestamp("20130102"),
            "C": pd.Series(1, index=list(range(4)), dtype="float32"),
            "D": np.array([3] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )
    from lineage.utils import infer_schema

    schema, source_schema = infer_schema(df, "foo")
    assert schema.shape == (4, 7)
    schema_converter = make_converter()
    datahub_schema = schema_converter.convert(source_schema)
    dataset_snapshot = target.make_dataset_snapshot(datahub_schema, schema)
    from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

    mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
    raw_mce_obj = mce.proposedSnapshot.to_obj()
    assert raw_mce_obj is not None


def test_make_task_snaphot():
    target = make_target()
    from lineage.interface import Pipeline, Task

    pipeline = Pipeline(id="1", name="p1")
    task = Task(
        id="1",
        name="news.workflows.covid.get",
        description="get",
        inputs=["covid"],
        outputs=["world_population"],
        url="https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp",
    )
    snapshot = target.make_task_snaphot(pipeline, task)
    assert (
        str(snapshot)
        == "DataJobSnapshotClass({'urn': 'urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.get)', 'aspects': [DataJobInfoClass({'customProperties': {}, 'externalUrl': 'https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp', 'name': 'news.workflows.covid.get', 'description': 'get', 'type': 'COMMAND', 'flowUrn': None, 'status': None}), DataJobInputOutputClass({'inputDatasets': ['urn:li:dataset:(urn:li:dataPlatform:flyte,covid,DEV)'], 'outputDatasets': ['urn:li:dataset:(urn:li:dataPlatform:flyte,world_population,DEV)'], 'inputDatajobs': None}), OwnershipClass({'owners': [], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None})})]})"
    )
    from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent

    mce = MetadataChangeEvent(proposedSnapshot=snapshot)
    raw_mce_obj = mce.proposedSnapshot.to_obj()
    assert raw_mce_obj is not None


def test_build_mce_pipeline():
    target = make_target()
    from lineage.interface import Pipeline, Task

    pipeline = Pipeline(
        id="1",
        name="p1",
        description="A wonderful job",
        url="https://foo.com",
        metadata=dict(a="x", b=2),
        tags=["sea", "beach"],
        owners=["xyz"],
    )

    task1 = Task(
        id="1",
        name="news.workflows.covid.get",
        description="get",
        inputs=["covid"],
        outputs=["world_population"],
        url="https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp",
    )
    task2 = Task(
        id="2",
        name="news.workflows.covid.filter_data",
        description="filter_data",
        inputs=["world_population"],
        outputs=["covid_population"],
        upstream_task_ids=[task1.name],
        url="https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp",
    )
    task3 = Task(
        id="3",
        name="news.workflows.covid.save",
        description="save",
        inputs=["covid_population"],
        upstream_task_ids=[task2.name],
        url="https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp",
    )
    task4 = Task(
        id="4",
        name="news.workflows.covid.final",
        description="final",
        inputs=[],
        upstream_task_ids=[task3.name],
        url="https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp",
    )
    pipeline.tasks = [task1, task2, task3, task4]
    mces = target.build_mce_pipeline(pipeline)
    assert len(mces) == 5
    assert (
        str(mces[0])
        == "MetadataChangeEventClass({'auditHeader': None, 'proposedSnapshot': DataFlowSnapshotClass({'urn': 'urn:li:dataFlow:(flyte,p1,DEV)', 'aspects': [DataFlowInfoClass({'customProperties': {'a': 'x', 'b': 2}, 'externalUrl': 'https://foo.com', 'name': 'p1', 'description': 'A wonderful job', 'project': None}), OwnershipClass({'owners': [OwnerClass({'owner': 'urn:li:corpuser:xyz', 'type': 'DATAOWNER', 'source': None})], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None})}), GlobalTagsClass({'tags': [TagAssociationClass({'tag': 'urn:li:tag:sea'}), TagAssociationClass({'tag': 'urn:li:tag:beach'})]})]}), 'proposedDelta': None, 'systemMetadata': None})"
    )
    assert (
        str(mces[1])
        == "MetadataChangeEventClass({'auditHeader': None, 'proposedSnapshot': DataJobSnapshotClass({'urn': 'urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.get)', 'aspects': [DataJobInfoClass({'customProperties': {}, 'externalUrl': 'https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp', 'name': 'news.workflows.covid.get', 'description': 'get', 'type': 'COMMAND', 'flowUrn': None, 'status': None}), DataJobInputOutputClass({'inputDatasets': ['urn:li:dataset:(urn:li:dataPlatform:flyte,covid,DEV)'], 'outputDatasets': ['urn:li:dataset:(urn:li:dataPlatform:flyte,world_population,DEV)'], 'inputDatajobs': None}), OwnershipClass({'owners': [], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None})})]}), 'proposedDelta': None, 'systemMetadata': None})"
    )
    assert (
        str(mces[2])
        == "MetadataChangeEventClass({'auditHeader': None, 'proposedSnapshot': DataJobSnapshotClass({'urn': 'urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.filter_data)', 'aspects': [DataJobInfoClass({'customProperties': {}, 'externalUrl': 'https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp', 'name': 'news.workflows.covid.filter_data', 'description': 'filter_data', 'type': 'COMMAND', 'flowUrn': None, 'status': None}), DataJobInputOutputClass({'inputDatasets': ['urn:li:dataset:(urn:li:dataPlatform:flyte,world_population,DEV)'], 'outputDatasets': ['urn:li:dataset:(urn:li:dataPlatform:flyte,covid_population,DEV)'], 'inputDatajobs': ['urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.get)']}), OwnershipClass({'owners': [], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None})})]}), 'proposedDelta': None, 'systemMetadata': None})"
    )
    assert (
        str(mces[3])
        == "MetadataChangeEventClass({'auditHeader': None, 'proposedSnapshot': DataJobSnapshotClass({'urn': 'urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.save)', 'aspects': [DataJobInfoClass({'customProperties': {}, 'externalUrl': 'https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp', 'name': 'news.workflows.covid.save', 'description': 'save', 'type': 'COMMAND', 'flowUrn': None, 'status': None}), DataJobInputOutputClass({'inputDatasets': ['urn:li:dataset:(urn:li:dataPlatform:flyte,covid_population,DEV)'], 'outputDatasets': [], 'inputDatajobs': ['urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.filter_data)']}), OwnershipClass({'owners': [], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None})})]}), 'proposedDelta': None, 'systemMetadata': None})"
    )
    assert (
        str(mces[4])
        == "MetadataChangeEventClass({'auditHeader': None, 'proposedSnapshot': DataJobSnapshotClass({'urn': 'urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.final)', 'aspects': [DataJobInfoClass({'customProperties': {}, 'externalUrl': 'https://flyte-poc.dev.aws.great.net/console/projects/poc/domains/development/executions/ydc4x7appp', 'name': 'news.workflows.covid.final', 'description': 'final', 'type': 'COMMAND', 'flowUrn': None, 'status': None}), DataJobInputOutputClass({'inputDatasets': [], 'outputDatasets': [], 'inputDatajobs': ['urn:li:dataJob:(urn:li:dataFlow:(flyte,p1,DEV),news.workflows.covid.save)']}), OwnershipClass({'owners': [], 'lastModified': AuditStampClass({'time': 0, 'actor': 'urn:li:corpuser:unknown', 'impersonator': None})})]}), 'proposedDelta': None, 'systemMetadata': None})"
    )
