import pyarrow as pa
import pandas as pd
import awswrangler as wr
import logging

from lineage import error_traceback
from lineage.interface import TargetSystem

logger = logging.getLogger(__name__)


class GlueCatalogTarget(TargetSystem):
    def __init__(self, bucket_path: str, kms_key_arn, db_name: str = None):
        self.db_name = db_name
        self.bucket_path = bucket_path
        self.kms_key_arn = kms_key_arn

    def db_name_from_pipeline(self, pipeline):
        # flytes pipeline name is domain.project.name i.e. development.poc.q5n0ogi5v
        # convert to flyte_development_poc for glue catalog db name
        parts = pipeline.name.split(".")
        return f"flyte_{parts[0]}_{parts[1]}"

    def ingest_dataset(self, pipeline, dataset, db_name):
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
            table=name,
            mode="overwrite",
            # description=desc,
            # parameters=param,
            # columns_comments=comments
        )
        logger.info(f"result: {result}")

    def ingest(self, pipeline, datasets):
        # use the configured db name if set otherwise use pipeline data
        db_name = self.db_name or self.db_name_from_pipeline(pipeline)
        databases = wr.catalog.databases()
        if db_name not in databases.values:
            logger.info(f"creating glue db: {db_name}")
            wr.catalog.create_database(db_name)
            logger.info(f"created glue db: {db_name}")

        for dataset in datasets:
            try:
                self.ingest_dataset(pipeline, dataset, db_name=db_name)
            except Exception as e:
                msg = f"Unable to ingest data set '{dataset[0].name}', error={e}, traceback={error_traceback()}"
                logger.warning(msg)
