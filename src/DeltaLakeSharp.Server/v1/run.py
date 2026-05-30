"""Entrypoint for the DeltaLakeSharp V1 Arrow Flight server."""

import argparse
import logging
import signal
import sys

from app.config import DEFAULT_FLIGHT_HOST, DEFAULT_FLIGHT_PORT
from app.v1.flight_server import DeltaFlightServer
from app.v1.spark_manager import SparkManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DeltaLakeSharp.V1")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DeltaLakeSharp V1 — Arrow Flight server backed by PySpark."
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_FLIGHT_PORT,
        help=f"Port to listen on (default: {DEFAULT_FLIGHT_PORT}).",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_FLIGHT_HOST,
        help=f"Host to bind to (default: {DEFAULT_FLIGHT_HOST}).",
    )
    args = parser.parse_args()

    location = f"grpc://{args.host}:{args.port}"

    spark_manager = SparkManager()

    # Eagerly create the SparkSession so startup failures are caught early.
    logger.info("Initializing SparkSession...")
    _ = spark_manager.spark
    logger.info("SparkSession ready.")

    server = DeltaFlightServer(spark_manager, location=location)

    def _shutdown(signum, frame):
        logger.info("Received signal %s, shutting down...", signum)
        server.shutdown()
        spark_manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Delta Table Service listening on %s", location)
    server.serve()


if __name__ == "__main__":
    main()
