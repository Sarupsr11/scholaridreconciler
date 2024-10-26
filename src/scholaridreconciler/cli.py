import logging

import click
import uvicorn

from scholaridreconciler.endpoint import app

logger = logging.getLogger(__name__)


@click.group()
@click.option("--debug/--no-debug", default=False, help="enable debug mode")
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"]),
    help="logging level; overwritten by --debug",
)
def cli(debug: bool, log_level: str):
    logger.debug(f"Starting {__name__} cli")
    logging.basicConfig(level=log_level, format="%(message)s")
    if debug:
        logger.setLevel(logging.DEBUG)


@cli.command()
@click.option("--port", default=8000, help="port the RESTful endpoint should be available on")
@click.option("--host", default="127.0.0.1", help="host address the RESTful endpoint should be available on")
def start(port: int, host: str):
    """start the RESTful server"""
    logger.debug(f"Starting RESTful endpoint at {host}:{port}")
    uvicorn.run(app, host=host, port=port)
