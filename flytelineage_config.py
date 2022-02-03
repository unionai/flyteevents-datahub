import logging
from lineage.datahub import DataHubTarget
from lineage.flyte import WorkflowEvents
from lineage.datahub import DataHubTarget
from lineage.glue import GlueCatalogTarget

c.FlyteLineage.sqs_queue = "103020-flyte-events2"
# c.FlyteLineage.aws_region = 'us-east-1'
c.FlyteLineage.log_format = (
    "%(asctime)s %(levelname)-5.5s [%(name)s][%(process)d:%(threadName)s] %(message)s"
)

# c.WorkflowEvents.emit = True

c.WorkflowEvents.datasets_only = False
c.WorkflowEvents.targets = [
    DataHubTarget(
        server="https://api.datahub.dev.aws.jpmchase.net", datasets_only=c.WorkflowEvents.datasets_only
    ),
    GlueCatalogTarget(
        bucket_path="app-id-103020-dep-id-103021-uu-id-5qqzivkh5yaj",
        kms_key_arn="arn:aws:kms:us-east-1:965012431333:key/941225e2-7a02-4afb-b54e-fa1aca7d34be",
    ),
]
