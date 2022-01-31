import logging
import logging.config
import sys
import os
from argparse import ArgumentParser
from lineage.interface import (
    TargetSystem,
)
from lineage.datahub import DataHubTarget, DatasetSchema
from lineage import error_traceback
from lineage.scripts import get_default_config, asbool
from lineage.flyte_events import (
    EventProcesser,
    WorkflowEvents,
    SQSSource,
)

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
        required=True,
        default="103020-flyte-events",
        help="SQS queue name or url",
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
        required=True,
        help="Datahub server url, e.g. https://api.datahub.dev.aws.great.net",
    )

    parser.add_argument(
        "--emit",
        dest="emit",
        default=True,
        help="Emit lineage to DataHub, default=True",
    )

    parser.add_argument(
        "--datasets-only",
        dest="datasets_only",
        default=False,
        help="Process datasets only not pipeline task lineage, default=False",
    )

    args = parser.parse_args()
    config = args.config or get_default_config()
    try:
        logging.config.fileConfig(config)
    except Exception as e:
        print(f"To configure use '-c path_to_config_file', error={e}")
        sys.exit(-1)

    emit = asbool(args.emit)
    datasets_only = asbool(args.datasets_only)
    try:
        event_processer = EventProcesser(
            sqs_source=SQSSource(name=args.sqs_queue, region_name=args.aws_region)
        )
        workflow = WorkflowEvents(
            targets=[DataHubTarget(server=args.datahub_server)],
            emit=emit,
            datasets_only=datasets_only,
        )
        event_processer.start(workflow=workflow)
    except Exception as e:
        msg = f"error: exception={e}, traceback={error_traceback()}"
        logger.error(msg)
        sys.exit(-1)
