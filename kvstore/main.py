"""
HyperKV Server Main Entry Point
Provides CLI interface for starting and managing the HyperKV server.
"""

import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path
from typing import Optional

from .core import HyperKVServer, HyperKVConfig


def setup_logging(level: str = "INFO", log_file: Optional[str] = None):
    """Set up logging configuration"""

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        # Create log directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Set specific logger levels
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def create_argument_parser() -> argparse.ArgumentParser:
    """Create command line argument parser"""

    parser = argparse.ArgumentParser(
        description="HyperKV - High-performance CRDT-compliant key-value store",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start with default configuration
  python -m kvstore.main
  
  # Start with custom config file
  python -m kvstore.main --config /path/to/config.yaml
  
  # Start with specific node ID and port
  python -m kvstore.main --node-id node-1 --port 6380
  
  # Start in cluster mode
  python -m kvstore.main --enable-clustering --node-id node-1
  
  # Start with debug logging
  python -m kvstore.main --log-level DEBUG
        """,
    )

    # Configuration options
    parser.add_argument(
        "--config", "-c", type=str, help="Path to YAML configuration file"
    )

    # Server options
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )

    parser.add_argument(
        "--port", "-p", type=int, default=6379, help="Port to listen on (default: 6379)"
    )

    parser.add_argument("--node-id", type=str, help="Unique node identifier")

    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Data directory path (default: ./data)",
    )

    # Memory and performance
    parser.add_argument(
        "--max-memory", type=str, help="Maximum memory usage (e.g., 1GB, 512MB)"
    )

    parser.add_argument(
        "--max-connections",
        type=int,
        default=10000,
        help="Maximum concurrent connections (default: 10000)",
    )

    parser.add_argument(
        "--eviction-policy",
        choices=["lru", "lfu", "arc", "random", "volatile-lru", "volatile-lfu"],
        default="lru",
        help="Cache eviction policy (default: lru)",
    )

    # Clustering
    parser.add_argument(
        "--enable-clustering",
        action="store_true",
        help="Enable distributed clustering with Raft consensus",
    )

    parser.add_argument(
        "--enable-replication", action="store_true", help="Enable data replication"
    )

    # Persistence
    parser.add_argument(
        "--storage-backend",
        choices=["memory", "rocksdb", "lmdb"],
        default="rocksdb",
        help="Storage backend (default: rocksdb)",
    )

    parser.add_argument(
        "--disable-aof",
        action="store_true",
        help="Disable Append-Only File persistence",
    )

    parser.add_argument(
        "--disable-snapshots", action="store_true", help="Disable snapshot persistence"
    )

    # Security
    parser.add_argument("--auth-password", type=str, help="Authentication password")

    parser.add_argument(
        "--enable-tls", action="store_true", help="Enable TLS encryption"
    )

    parser.add_argument("--tls-cert", type=str, help="TLS certificate file path")

    parser.add_argument("--tls-key", type=str, help="TLS private key file path")

    # Logging and debugging
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--log-file", type=str, help="Log file path (logs to stdout if not specified)"
    )

    parser.add_argument(
        "--enable-profiling", action="store_true", help="Enable performance profiling"
    )

    # Daemon mode
    parser.add_argument("--daemon", action="store_true", help="Run as daemon process")

    parser.add_argument("--pid-file", type=str, help="PID file path for daemon mode")

    return parser


def parse_memory_size(memory_str: str) -> int:
    """Parse memory size string to bytes"""
    if not memory_str:
        return None

    memory_str = memory_str.upper().strip()

    # Extract number and unit
    import re

    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?B?)$", memory_str)

    if not match:
        raise ValueError(f"Invalid memory size format: {memory_str}")

    size = float(match.group(1))
    unit = match.group(2) or "B"

    # Convert to bytes
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1024**2,
        "GB": 1024**3,
        "TB": 1024**4,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }

    return int(size * multipliers[unit])


def create_config_from_args(args: argparse.Namespace) -> HyperKVConfig:
    """Create HyperKVConfig from command line arguments"""

    # Start with config file if provided
    if args.config:
        config = HyperKVConfig.from_yaml(args.config)
    else:
        config = HyperKVConfig()

    # Override with command line arguments
    if args.host:
        config.host = args.host

    if args.port:
        config.port = args.port

    if args.node_id:
        config.node_id = args.node_id

    if args.data_dir:
        config.data_dir = args.data_dir

    if args.max_memory:
        config.max_memory = parse_memory_size(args.max_memory)

    if args.max_connections:
        config.max_connections = args.max_connections

    if args.eviction_policy:
        config.eviction_policy = args.eviction_policy

    if args.enable_clustering:
        config.enable_clustering = True

    if args.enable_replication:
        config.enable_replication = True

    if args.storage_backend:
        config.storage_backend = args.storage_backend

    if args.disable_aof:
        config.aof_enabled = False

    if args.disable_snapshots:
        config.snapshot_enabled = False

    if args.auth_password:
        config.auth_password = args.auth_password

    if args.enable_tls:
        config.enable_tls = True

    if args.tls_cert:
        config.tls_cert_file = args.tls_cert

    if args.tls_key:
        config.tls_key_file = args.tls_key

    if args.log_file:
        config.log_file = args.log_file

    if args.log_level:
        config.log_level = args.log_level

    if args.enable_profiling:
        config.enable_profiling = True

    return config


def run_as_daemon(pid_file: Optional[str] = None):
    """Run the process as a daemon"""
    import os
    import sys

    # First fork
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process, exit
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"fork #1 failed: {e}\n")
        sys.exit(1)

    # Decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # Second fork
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process, exit
            sys.exit(0)
    except OSError as e:
        sys.stderr.write(f"fork #2 failed: {e}\n")
        sys.exit(1)

    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()

    si = open(os.devnull, "r")
    so = open(os.devnull, "a+")
    se = open(os.devnull, "a+")

    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # Write PID file
    if pid_file:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))


async def main():
    """Main entry point"""

    # Parse arguments
    parser = create_argument_parser()
    args = parser.parse_args()

    # Set up logging first
    setup_logging(args.log_level, args.log_file)

    logger = logging.getLogger(__name__)

    try:
        # Create configuration
        config = create_config_from_args(args)

        logger.info("HyperKV Server starting...")
        logger.info(f"Configuration: {config}")

        # Run as daemon if requested
        if args.daemon:
            logger.info("Running as daemon...")
            run_as_daemon(args.pid_file)

            # Re-setup logging after daemon fork
            setup_logging(args.log_level, args.log_file)
            logger = logging.getLogger(__name__)

        # Create and start server
        server = HyperKVServer(config)

        try:
            await server.start()

            logger.info("HyperKV Server started successfully")
            logger.info(f"Listening on {config.host}:{config.port}")

            if config.enable_clustering:
                logger.info("Clustering enabled with Raft consensus")

            if config.enable_metrics:
                logger.info(f"Metrics available on port {config.metrics_port}")

            # Wait for shutdown
            await server.wait_for_shutdown()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        except Exception as e:
            logger.error(f"Server error: {e}")
            raise
        finally:
            await server.stop()

    except Exception as e:
        logger.error(f"Failed to start HyperKV Server: {e}")
        sys.exit(1)

    logger.info("HyperKV Server stopped")


def sync_main():
    """Synchronous main entry point"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    sync_main()
