import logging
import logging.config
import sys
import os
from argparse import ArgumentParser
from lineage import error_traceback
from lineage.scripts import get_default_config, asbool
from lineage.flyte import (
    FlyteLineage,
    WorkflowEvents,
)

logger = logging.getLogger(__name__)


def lineage_cmd(argv=None):
    try:
        flytelineage = FlyteLineage.instance()
        flytelineage.initialize(argv)
        workflow = WorkflowEvents.instance()
        workflow.update_config(flytelineage.config)
        flytelineage.start(workflow=workflow)
    except Exception as e:
        msg = f"error: exception={e}, traceback={error_traceback()}"
        logger.error(msg)
        sys.exit(-1)
