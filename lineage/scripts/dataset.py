import logging
import logging.config
import typing
import sys
from argparse import ArgumentParser
from pathlib import Path
from pyarrow import csv, json, parquet
from lineage.interface import (
    TargetSystem,
)
from lineage.datahub import DataHubTarget
from lineage.datahub import DatasetSchema
from lineage.utils import infer_schema
from lineage import error_traceback
from . import (
    get_default_config,
)


logger = logging.getLogger(__name__)


def get_list_arg(arg):
    arg = arg or []
    if arg:
        arg = arg.split(",")
    return arg


def dataset_cmd():
    parser = ArgumentParser(description="Emit Source Dataset -> Datahub")
    parser.add_argument(
        "-p",
        "--platform",
        dest="platform",
        default="flyte",
        help="Source platform, default=flyte",
    )
    parser.add_argument(
        "-s",
        "--server",
        dest="server",
        default="https://api.datahub.dev.aws.jpmchase.net",
        help="Datahub server url, default=https://api.datahub.dev.aws.jpmchase.net",
    )

    parser.add_argument("-f", "--filepath", dest="filepath", help="dataset filepath")
    parser.add_argument(
        "--filetype",
        dest="filetype",
        default="csv",
        help="file type format, one of csv, json or parquet",
    )
    parser.add_argument("-n", "--name", dest="name", help="dataset name")
    parser.add_argument(
        "-d", "--description", dest="description", help="dataset description"
    )
    parser.add_argument(
        "-o",
        "--owners",
        dest="owners",
        help="comma separated list of owners e.g. joe,clair",
    )
    parser.add_argument(
        "-t",
        "--tags",
        dest="tags",
        help="comma separated list of tags e.g. wonderful,beauty",
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        help="Path to configuration file (defaults to $CWD/etc/dev.ini)",
        metavar="FILE",
    )

    args = parser.parse_args()
    config = args.config or get_default_config()
    try:
        logging.config.fileConfig(config)
    except:
        print(f"Unable to configure logging use '-c path_to_config_file' to configure")

    try:
        filepath = args.filepath
        name = args.name
        owners = get_list_arg(args.owners)
        tags = get_list_arg(args.tags)
        filetype = args.filetype
        logger.info(f"{name}: {filepath}")
        filetypes = ("csv", "json", "parquet")
        if filetype == "csv":
            df = csv.read_csv(filepath)
        elif filetype == "json":
            df = json.read_json(filepath)
        elif filetype == "parquet":
            df = parquet.read_table(filepath)
        else:
            raise ValueError(f"filetype must be one of {filetypes}")
        target = DataHubTarget(server=args.server, platform=args.platform)
        dataset_schema, source_schema = infer_schema(
            df, name, description=args.description, owners=owners, tags=tags
        )
        schema_converter = target.make_schema_converter()
        datahub_schema = schema_converter.convert(source_schema)
        target.emit_dataset(datahub_schema, dataset_schema)
    except Exception as e:
        logger.error("exception: error={}, traceback={}".format(e, error_traceback()))
        sys.exit(-1)
