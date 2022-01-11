import logging
import logging.config
import sys
from argparse import ArgumentParser
from lineage.interface import (
    TargetSystem,
)
from lineage.datahub import DataHubTarget, DatasetSchema
from lineage import error_traceback
from lineage.scripts import get_default_config, asbool
from lineage.flyte import EventProcesser, Workflow, SQSSource

logger = logging.getLogger(__name__)


def lineage_cmd():
    parser = ArgumentParser(description="Emit Flyte data lineage -> DataHub")
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        help="Path to configuration file (defaults to $CWD/etc/dev.ini)",
        metavar="FILE",
    )
    parser.add_argument(
        "--sqs_queue",
        dest="sqs_queue",
        default="103020-flyte-events",
        help="SQS queue name, default=103020-flyte-events",
    )
    parser.add_argument(
        "--aws_region",
        dest="aws_region",
        default="us-east-1",
        help="AWS region, default=us-east-1",
    )

    parser.add_argument(
        "--datahub_server",
        dest="datahub_server",
        default="https://api.datahub.dev.aws.jpmchase.net",
        help="Datahub server url, default=https://api.datahub.dev.aws.jpmchase.net",
    )

    parser.add_argument(
        "--emit",
        dest="emit",
        default=True,
        help="Emit lineage to DataHub, default=True",
    )

    args = parser.parse_args()
    config = args.config or get_default_config()
    try:
        logging.config.fileConfig(config)
    except:
        print("Unable to configure logging use '-c path_to_config_file' to configure.")
        sys.exit(-1)

    emit = asbool(args.emit)
    try:
        event_processer = EventProcesser(
            sqs_source=SQSSource(name=args.sqs_queue, region_name=args.aws_region)
        )
        workflow = Workflow(target=DataHubTarget(server=args.datahub_server), emit=emit)
        event_processer.start(workflow=workflow)
    except Exception as e:
        msg = "exception: error={}, traceback={}".format(e, error_traceback())
        logger.error(msg)
        sys.exit(-1)
