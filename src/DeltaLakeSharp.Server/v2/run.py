"""Entrypoint for the DeltaLakeSharp V2 Arrow Flight server (DataFusion)."""

import argparse
import logging
import signal
import sys

from app.config import DEFAULT_FLIGHT_HOST, DEFAULT_FLIGHT_PORT
from app.v2.flight_server import DeltaFlightServerV2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DeltaLakeSharp.V2")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DeltaLakeSharp V2 — Arrow Flight server backed by DataFusion + delta-rs."
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

    server = DeltaFlightServerV2(location=location)

    def _shutdown(signum, frame):
        logger.info("Received signal %s, shutting down...", signum)
        server.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Delta Table Service V2 (DataFusion) listening on %s", location)
    server.serve()


if __name__ == "__main__":
    main()
