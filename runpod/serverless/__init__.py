"""Allows serverless to be recognized as a package."""

import os
import sys
import json
import time
import asyncio
import argparse

from . import work_loop
from .modules import rp_fastapi
from .modules.rp_logger import RunPodLogger

log = RunPodLogger()

# ---------------------------------------------------------------------------- #
#                              Run Time Arguments                              #
# ---------------------------------------------------------------------------- #
# Arguments will be passed in with the config under the key "rp_args"
parser = argparse.ArgumentParser(
    prog="runpod",
    description="Runpod Serverless Worker Arguments."
)
parser.add_argument("--test_input", type=str, default=None,
                    help="Test input for the worker, formatted as JSON.")
parser.add_argument("--rp_debugger", action="store_true", default=None,
                    help="Flag to enable the Debugger.")
parser.add_argument("rp_log_level", default=None,
                    help="""Controls what level of logs are printed to the console.
                    Options: ERROR, WARN, INFO, and DEBUG.""")


def _set_config_args(config) -> dict:
    """
    Sets the config rp_args, removing any recognized arguments from sys.argv.
    Returns: config
    """
    args, unknown = parser.parse_known_args()
    sys.argv = [sys.argv[0]] + unknown

    # Directly assign the parsed arguments to config
    config["rp_args"] = vars(args)

    # Parse the test input from JSON
    if config["rp_args"]["test_input"]:
        config["rp_args"]["test_input"] = json.loads(config["rp_args"]["test_input"])

    # Set the log level
    if config["rp_args"]["rp_log_level"]:
        log.set_level(config["rp_args"]["rp_debug_level"])

    return config


def _get_realtime_port() -> int:
    """
    Get the realtime port from the environment variable if it exists.
    """
    return int(os.environ.get("RUNPOD_REALTIME_PORT", "0"))


def _get_realtime_concurrency() -> int:
    """
    Get the realtime concurrency from the environment variable if it exists.
    """
    return int(os.environ.get("RUNPOD_REALTIME_CONCURRENCY", "1"))


# ---------------------------------------------------------------------------- #
#                            Start Serverless Worker                           #
# ---------------------------------------------------------------------------- #
def start(config):
    """
    Starts the serverless worker.
    """
    config["reference_counter_start"] = time.perf_counter()
    config = _set_config_args(config)

    realtime_port = _get_realtime_port()
    realtime_concurrency = _get_realtime_concurrency()

    if realtime_port:
        api_server = rp_fastapi.WorkerAPI()
        api_server.config = config

        api_server.start_uvicorn(realtime_port, realtime_concurrency)

    else:
        asyncio.run(work_loop.start_worker(config))
