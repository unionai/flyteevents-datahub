from typing import List, Tuple, Any
import awswrangler as wr
import logging

from flytelineage import error_traceback
from .interface import TargetSystem, Pipeline
from .dataset import DatasetSchema
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)


class GlueCatalogTarget(TargetSystem):
    """AWS Glue target lineage system

    ``bucket_path`` S3 glue bucket path e.g. my-glue-bucket
    ``kms_key_arn`` kms key arn used for S3 access
    ``versioning`` set to True to enable schema versioning, defaults to False
    """

    def __init__(
        self,
        bucket_path: str,
        kms_key_arn: str,
        db_name: str = None,
        versioning: bool = False,
    ):
        self.db_name = db_name
        self.bucket_path = bucket_path
        self.kms_key_arn = kms_key_arn
        self.versioning = versioning
        self.schema_evolution = True if self.versioning else False

    def db_name_from_pipeline(self, pipeline: Pipeline) -> str:
        # flytes pipeline name is domain.project.name i.e. development.poc.q5n0ogi5v
        # convert to flyte_development_poc for glue catalog db name
        parts = pipeline.name.split(".")
        return f"flyte_{parts[0]}_{parts[1]}"

    def normalize_name(self, name: str) -> str:
        """Normalize the table name for glue which does not support '.' chars."""
        return name.replace(".", "_")

    def ingest_dataset(
        self,
        pipeline: Pipeline,
        dataset: Tuple[DatasetSchema, pa.Schema, pd.DataFrame],
        db_name: str,
    ):
        dataset_schema, source_schema, df = dataset
        name = dataset_schema.name
        # Note: using the same bucket for multiple dbs so including db in the path
        result = wr.s3.to_parquet(
            df=df,
            path=f"s3://{self.bucket_path}/{db_name}/{name}/",
            s3_additional_kwargs={
                "ServerSideEncryption": "aws:kms",
                "SSEKMSKeyId": self.kms_key_arn,
            },
            dataset=True,
            database=db_name,
            table=self.normalize_name(name),
            mode="overwrite",
            catalog_versioning=self.versioning,
            schema_evolution=self.schema_evolution,
        )
        logger.info(f"s3 write returned: {result}")
        return result

    def ingest(
        self,
        pipeline: Pipeline,
        datasets: List[Tuple[DatasetSchema, pa.Schema, pd.DataFrame]],
    ) -> Any:
        # use the configured db name if set otherwise use pipeline data
        db_name = self.db_name or self.db_name_from_pipeline(pipeline)
        databases = wr.catalog.databases()
        if db_name not in databases.values:
            logger.info(f"creating glue db: {db_name}")
            wr.catalog.create_database(db_name)
            logger.info(f"created glue db: {db_name}")
        result = []
        for dataset in datasets:
            try:
                result.append(self.ingest_dataset(pipeline, dataset, db_name=db_name))
            except Exception as e:
                msg = f"Unable to ingest data set '{dataset[0].name}', error={e}, traceback={error_traceback()}"
                logger.warning(msg)
        return result
