import typing
import logging
import pandas as pd
import pyarrow as pa
from .dataset import DatasetSchema
from .interface import TargetSystem, SchemaConverter, Pipeline, Task
from lineage import error_traceback
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter import mce_builder
from datahub.ingestion.api.source import Source
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp, Status
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
    GlobalTagsClass,
    DataJobSnapshotClass,
    DataJobInputOutputClass,
    DataJobInfoClass,
    AzkabanJobTypeClass,
    MetadataChangeEventClass,
    DataFlowSnapshotClass,
    DataFlowInfoClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    FixedTypeClass,
    MapTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    StringTypeClass,
    TimeTypeClass,
    UnionTypeClass,
    SchemaMetadata,
    SchemalessClass,
    OtherSchema,
)

logger = logging.getLogger(__name__)
T = typing.TypeVar("T")


def tags(tags: typing.List[str]):
    return GlobalTagsClass(tags=[TagAssociationClass(f"urn:li:tag:{x}") for x in tags])


def audit_stamp(dt, platform):
    actor = f"urn:li:corpuser:{platform}_executor"
    return AuditStamp(
        time=int(dt.timestamp() * 1000),
        actor=actor,
    )


def owners(owners: typing.List[str], ownership_type=None):
    ownership_type = ownership_type or OwnershipTypeClass.DATAOWNER
    return OwnershipClass(
        owners=[
            OwnerClass(owner=f"urn:li:corpuser:{x}", type=ownership_type)
            for x in owners
        ],
    )


class DataHubSchemaConverter(SchemaConverter):
    # pyarrow.lib.Field.type to DataHub classes
    _field_type_mapping: typing.Dict[str, T] = {
        # scalars
        "None": NullTypeClass,
        "null": NullTypeClass,
        "bool": BooleanTypeClass,
        "int8": NumberTypeClass,
        "int16": NumberTypeClass,
        "int32": NumberTypeClass,
        "int64": NumberTypeClass,
        "uint8": NumberTypeClass,
        "uint16": NumberTypeClass,
        "uint32": NumberTypeClass,
        "uint64": NumberTypeClass,
        "halffloat": NumberTypeClass,
        "float": NumberTypeClass,
        "double": NumberTypeClass,
        "string": StringTypeClass,
        "time32[s]": TimeTypeClass,
        "time64[us]": TimeTypeClass,
        "timestamp[s]": TimeTypeClass,
        "timestamp[ms]": TimeTypeClass,
        "timestamp[us]": TimeTypeClass,
        "timestamp[ns]": TimeTypeClass,
        "duration[s]": TimeTypeClass,
        "duration[ms]": TimeTypeClass,
        "duration[us]": TimeTypeClass,
        "duration[ns]": TimeTypeClass,
        "date32[day]": DateTypeClass,
        "date64[ms]": DateTypeClass,
        "binary": BytesTypeClass,
    }

    def get_complex_type(self, source_field):
        source_type = str(source_field.type)
        target_class_type = None
        if source_type.startswith("list"):
            target_class_type = ArrayTypeClass
        elif source_type.startswith("dictionary"):
            target_class_type = MapTypeClass
        # TODO: support other complex types (struct etc)
        return target_class_type

    def convert(self, source_schema: pa.Schema) -> typing.List:
        """Convert source schema represented by pyarrow schema to DataHub. Error if any
        field fails conversion.
        https://arrow.apache.org/docs/python/generated/pyarrow.Field.html#pyarrow.Field
        """
        schema = []
        for source_field in source_schema:
            source_type = str(source_field.type)
            if source_type.startswith("list") or source_type.startswith("dictionary"):
                datahub_type_cls = self.get_complex_type(source_field)
            else:
                datahub_type_cls = self._field_type_mapping.get(source_type)
            if not datahub_type_cls:
                err_msg = f"Unable to convert source pyarrow schema field '{source_field}' to DataHub"
                logger.warning(err_msg)
                # its all or nothing
                raise ValueError(err_msg)
            field = SchemaField(
                fieldPath=source_field.name,
                nativeDataType=source_type,
                type=SchemaFieldDataType(type=datahub_type_cls()),
                description=source_field.name,
                recursive=False,
                nullable=source_field.nullable,
                # isPartOfKey=False
                # globalTags= GlobalTagsClass(tags=[TagAssociationClass(f"urn:li:tag:{tag}") for tag in tags])
            )
            schema.append(field)
        return schema


class DataHubTarget(TargetSystem):
    def __init__(
        self,
        server: str,
        platform: str = "flyte",
        env: str = "DEV",
        datasets_only=False,
        token=None,
        connect_timeout=None,
        read_timeout=None,
        extra_headers=None,
        test_connection=True,
        just_testing=False,
    ):
        self.emitter = DatahubRestEmitter(
            server,
            token=token,
            connect_timeout_sec=connect_timeout,
            read_timeout_sec=read_timeout,
            extra_headers=extra_headers,
        )
        if test_connection:
            self.emitter.test_connection()
        self.env = env
        self.platform = platform
        self.datasets_only = datasets_only
        self.just_testing = just_testing

    def ingest(self, pipeline, datasets):
        logger.info(f"DataHub ingestion for {pipeline.name}")
        for dataset_info in datasets:
            dataset_schema, source_schema, _ = dataset_info
            try:
                # emit dataset as its own entity
                schema_converter = DataHubSchemaConverter()
                target_schema = schema_converter.convert(source_schema)
                logger.info(f"emitting dataset {dataset_schema.name}")
                self.emit_dataset(target_schema, dataset_schema)
                logger.info("emitted dataset")
            except Exception as e:
                msg = f"Unable to ingest data set '{dataset_schema.name}', error={e}, traceback={error_traceback()}"
                logger.warning(msg)
        if not self.datasets_only:
            logger.info(f"emit pipeline: {pipeline.name}")
            self.emit_pipeline(pipeline)

    def make_dataset_snapshot(
        self, schema: typing.List[SchemaField], dataset_schema: DatasetSchema
    ):
        platform = self.platform
        env = self.env
        logger.info(f"creating dataset snapshot: {dataset_schema.name}")
        logger.debug(f"dataset: {dataset_schema}, datahub: {schema}")
        dataset_urn = mce_builder.make_dataset_urn(platform, dataset_schema.name, env)
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[],
        )
        dataset_properties = DatasetPropertiesClass(
            description=dataset_schema.description,
            # tags=tags(dataset_schema.tags),
            customProperties=dataset_schema.merge_metadata(),
        )
        dataset_snapshot.aspects.append(dataset_properties)
        # QU: what does Status do?
        dataset_snapshot.aspects.append(Status(removed=False))
        dataset_snapshot.aspects.append(owners(dataset_schema.owners))
        if dataset_schema.tags:
            dataset_snapshot.aspects.append(
                mce_builder.make_global_tag_aspect_with_tag_list(dataset_schema.tags)
            )

        schema_metadata = SchemaMetadata(
            schemaName=dataset_schema.name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=dataset_schema.version,
            hash=dataset_schema.hash(),
            platformSchema=OtherSchema(rawSchema=str(dataset_schema.schema)),
            fields=schema,
            # foreignKeys
            # primaryKeys
        )

        if dataset_schema.last_modified:
            schema_metadata.lastModified = audit_stamp(
                dataset_schema.last_modified, platform
            )
        if dataset_schema.created:
            schema_metadata.created = audit_stamp(dataset_schema.created, platform)

        dataset_snapshot.aspects.append(schema_metadata)
        return dataset_snapshot

    def emit_mce(self, mce: MetadataChangeEvent):
        logger.debug(f"mce: {mce}")
        if not self.just_testing:
            self.emitter.emit_mce(mce)

    def emit_dataset(
        self,
        schema: typing.List[SchemaField],
        dataset_schema: DatasetSchema,
        dataset: pd.DataFrame = None,
    ):
        dataset_snapshot = self.make_dataset_snapshot(schema, dataset_schema)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        self.emit_mce(mce)

    def make_task_snapshot(self, pipeline: Pipeline, task: Task):
        platform = self.platform
        env = self.env
        job_urn = mce_builder.make_data_job_urn(platform, pipeline.name, task.name, env)
        job_snapshot = DataJobSnapshotClass(
            urn=job_urn,
            aspects=[],
        )

        job_info = DataJobInfoClass(
            name=task.name,
            type=AzkabanJobTypeClass.COMMAND,
            description=task.description,
            customProperties=task.metadata,
            externalUrl=task.url,
        )

        upstream_tasks = [
            mce_builder.make_data_job_urn(platform, pipeline.name, task_id, env)
            for task_id in task.upstream_task_ids
        ]

        job_io = DataJobInputOutputClass(
            inputDatasets=[
                mce_builder.make_dataset_urn(platform, x, env) for x in task.inputs
            ],
            outputDatasets=[
                mce_builder.make_dataset_urn(platform, x, env) for x in task.outputs
            ],
        )

        if upstream_tasks:
            job_io.inputDatajobs = upstream_tasks

        job_snapshot.aspects.append(job_info)
        job_snapshot.aspects.append(job_io)

        if task.tags:
            job_snapshot.aspects.append(
                mce_builder.make_global_tag_aspect_with_tag_list(task.tags)
            )
        job_snapshot.aspects.append(owners(task.owners))
        return job_snapshot

    def make_pipeline_snapshot(self, pipeline: Pipeline):
        flow_urn = mce_builder.make_data_flow_urn(
            self.platform, pipeline.name, self.env
        )
        return DataFlowSnapshotClass(
            urn=flow_urn,
            aspects=[
                DataFlowInfoClass(
                    name=pipeline.name,
                    description=pipeline.description,
                    customProperties=pipeline.metadata,
                    externalUrl=pipeline.url,
                ),
                owners(pipeline.owners),
                mce_builder.make_global_tag_aspect_with_tag_list(pipeline.tags),
            ],
        )

    def build_mce_pipeline(self, pipeline: Pipeline):
        # A flow (pipeline) can have many jobs (tasks)
        flow_mce = MetadataChangeEventClass(
            proposedSnapshot=self.make_pipeline_snapshot(pipeline)
        )
        task_mces = []
        for task in pipeline.tasks:
            task_snapshot = self.make_task_snapshot(pipeline, task)
            task_mces.append(MetadataChangeEvent(proposedSnapshot=task_snapshot))
        return [flow_mce] + task_mces

    def emit_task(self, pipeline: Pipeline, task: Task):
        snapshot = self.make_task_snapshot(pipeline, task)
        self.emit_mce(MetadataChangeEvent(proposedSnapshot=snapshot))

    def emit_pipeline(self, pipeline: Pipeline):
        mces = self.build_mce_pipeline(pipeline)
        for mce in mces:
            self.emit_mce(mce)
