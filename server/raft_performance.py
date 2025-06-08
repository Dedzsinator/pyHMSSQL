"""Performance optimization configuration for RAFT consensus.

This module provides tuned parameters for different deployment scenarios
and workload characteristics to optimize RAFT performance.
"""

from enum import Enum
from typing import Dict, Any
from dataclasses import dataclass


class DeploymentType(Enum):
    """Different deployment scenarios"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    HIGH_AVAILABILITY = "high_availability"


class WorkloadType(Enum):
    """Different workload characteristics"""
    OLTP = "oltp"              # Online Transaction Processing
    OLAP = "olap"              # Online Analytical Processing
    MIXED = "mixed"            # Mixed workload
    READ_HEAVY = "read_heavy"  # Read-heavy workload
    WRITE_HEAVY = "write_heavy" # Write-heavy workload


@dataclass
class RaftPerformanceConfig:
    """RAFT performance configuration"""

    # Election timeouts (milliseconds)
    election_timeout_min: int
    election_timeout_max: int

    # Heartbeat interval (milliseconds)
    heartbeat_interval: int

    # Log replication settings
    max_log_entries_per_request: int
    log_compaction_threshold: int

    # Network settings
    network_timeout: int
    max_retries: int
    retry_backoff_base: int

    # Batch processing
    batch_size: int
    batch_timeout: int

    # Memory management
    log_cache_size: int
    snapshot_threshold: int

    # Performance tuning
    parallel_append_entries: bool
    async_log_writes: bool
    compression_enabled: bool

    # Monitoring
    metrics_collection_interval: int
    health_check_interval: int


class RaftPerformanceTuner:
    """Optimizes RAFT parameters for different scenarios"""

    CONFIGURATIONS = {
        # Development configuration - fast feedback, less durability
        (DeploymentType.DEVELOPMENT, WorkloadType.MIXED): RaftPerformanceConfig(
            election_timeout_min=150,
            election_timeout_max=300,
            heartbeat_interval=50,
            max_log_entries_per_request=100,
            log_compaction_threshold=1000,
            network_timeout=1000,
            max_retries=3,
            retry_backoff_base=100,
            batch_size=50,
            batch_timeout=10,
            log_cache_size=1000,
            snapshot_threshold=5000,
            parallel_append_entries=True,
            async_log_writes=True,
            compression_enabled=False,
            metrics_collection_interval=30,
            health_check_interval=10
        ),

        # Testing configuration - balanced settings
        (DeploymentType.TESTING, WorkloadType.MIXED): RaftPerformanceConfig(
            election_timeout_min=200,
            election_timeout_max=400,
            heartbeat_interval=75,
            max_log_entries_per_request=200,
            log_compaction_threshold=5000,
            network_timeout=2000,
            max_retries=5,
            retry_backoff_base=150,
            batch_size=100,
            batch_timeout=20,
            log_cache_size=5000,
            snapshot_threshold=25000,
            parallel_append_entries=True,
            async_log_writes=True,
            compression_enabled=True,
            metrics_collection_interval=60,
            health_check_interval=15
        ),

        # Production OLTP - optimized for low latency
        (DeploymentType.PRODUCTION, WorkloadType.OLTP): RaftPerformanceConfig(
            election_timeout_min=300,
            election_timeout_max=600,
            heartbeat_interval=100,
            max_log_entries_per_request=500,
            log_compaction_threshold=10000,
            network_timeout=3000,
            max_retries=5,
            retry_backoff_base=200,
            batch_size=200,
            batch_timeout=5,  # Low latency
            log_cache_size=10000,
            snapshot_threshold=50000,
            parallel_append_entries=True,
            async_log_writes=True,
            compression_enabled=True,
            metrics_collection_interval=60,
            health_check_interval=20
        ),

        # Production OLAP - optimized for throughput
        (DeploymentType.PRODUCTION, WorkloadType.OLAP): RaftPerformanceConfig(
            election_timeout_min=500,
            election_timeout_max=1000,
            heartbeat_interval=150,
            max_log_entries_per_request=1000,
            log_compaction_threshold=20000,
            network_timeout=5000,
            max_retries=7,
            retry_backoff_base=300,
            batch_size=1000,  # High throughput
            batch_timeout=50,
            log_cache_size=50000,
            snapshot_threshold=100000,
            parallel_append_entries=True,
            async_log_writes=True,
            compression_enabled=True,
            metrics_collection_interval=120,
            health_check_interval=30
        ),

        # Production Read-Heavy - optimized for read performance
        (DeploymentType.PRODUCTION, WorkloadType.READ_HEAVY): RaftPerformanceConfig(
            election_timeout_min=400,
            election_timeout_max=800,
            heartbeat_interval=120,
            max_log_entries_per_request=300,
            log_compaction_threshold=15000,
            network_timeout=4000,
            max_retries=6,
            retry_backoff_base=250,
            batch_size=150,
            batch_timeout=15,
            log_cache_size=25000,  # Larger cache for reads
            snapshot_threshold=75000,
            parallel_append_entries=True,
            async_log_writes=True,
            compression_enabled=True,
            metrics_collection_interval=90,
            health_check_interval=25
        ),

        # Production Write-Heavy - optimized for write performance
        (DeploymentType.PRODUCTION, WorkloadType.WRITE_HEAVY): RaftPerformanceConfig(
            election_timeout_min=250,
            election_timeout_max=500,
            heartbeat_interval=80,
            max_log_entries_per_request=750,
            log_compaction_threshold=8000,
            network_timeout=2500,
            max_retries=4,
            retry_backoff_base=150,
            batch_size=500,  # Large batches for writes
            batch_timeout=8,   # Quick batching
            log_cache_size=15000,
            snapshot_threshold=40000,
            parallel_append_entries=True,
            async_log_writes=True,
            compression_enabled=True,
            metrics_collection_interval=45,
            health_check_interval=15
        ),

        # High Availability - optimized for fault tolerance
        (DeploymentType.HIGH_AVAILABILITY, WorkloadType.MIXED): RaftPerformanceConfig(
            election_timeout_min=600,
            election_timeout_max=1200,
            heartbeat_interval=200,
            max_log_entries_per_request=400,
            log_compaction_threshold=12000,
            network_timeout=6000,
            max_retries=10,  # More retries for HA
            retry_backoff_base=500,
            batch_size=250,
            batch_timeout=25,
            log_cache_size=20000,
            snapshot_threshold=60000,
            parallel_append_entries=True,
            async_log_writes=False,  # Synchronous for durability
            compression_enabled=True,
            metrics_collection_interval=30,  # Frequent monitoring
            health_check_interval=10
        )
    }

    @classmethod
    def get_configuration(cls, deployment_type: DeploymentType,
                         workload_type: WorkloadType) -> RaftPerformanceConfig:
        """Get optimized configuration for deployment and workload type"""
        config_key = (deployment_type, workload_type)

        if config_key in cls.CONFIGURATIONS:
            return cls.CONFIGURATIONS[config_key]

        # Fallback to production mixed workload
        fallback_key = (DeploymentType.PRODUCTION, WorkloadType.MIXED)
        if fallback_key in cls.CONFIGURATIONS:
            return cls.CONFIGURATIONS[fallback_key]

        # Default configuration
        return cls._get_default_configuration()

    @classmethod
    def _get_default_configuration(cls) -> RaftPerformanceConfig:
        """Get default configuration"""
        return RaftPerformanceConfig(
            election_timeout_min=300,
            election_timeout_max=600,
            heartbeat_interval=100,
            max_log_entries_per_request=300,
            log_compaction_threshold=10000,
            network_timeout=3000,
            max_retries=5,
            retry_backoff_base=200,
            batch_size=200,
            batch_timeout=20,
            log_cache_size=10000,
            snapshot_threshold=50000,
            parallel_append_entries=True,
            async_log_writes=True,
            compression_enabled=True,
            metrics_collection_interval=60,
            health_check_interval=20
        )

    @classmethod
    def auto_tune(cls, cluster_size: int, network_latency: float,
                  cpu_cores: int, memory_gb: int) -> RaftPerformanceConfig:
        """Automatically tune configuration based on system characteristics"""
        base_config = cls._get_default_configuration()

        # Adjust for cluster size
        if cluster_size >= 7:
            # Larger clusters need longer timeouts
            base_config.election_timeout_min = int(base_config.election_timeout_min * 1.5)
            base_config.election_timeout_max = int(base_config.election_timeout_max * 1.5)
            base_config.heartbeat_interval = int(base_config.heartbeat_interval * 1.3)

        # Adjust for network latency (in milliseconds)
        if network_latency > 10:  # High latency network
            base_config.network_timeout = int(base_config.network_timeout * 2)
            base_config.election_timeout_min = int(base_config.election_timeout_min * 1.8)
            base_config.election_timeout_max = int(base_config.election_timeout_max * 1.8)
            base_config.max_retries = min(base_config.max_retries + 3, 15)

        # Adjust for CPU cores
        if cpu_cores >= 8:
            base_config.parallel_append_entries = True
            base_config.batch_size = min(base_config.batch_size * 2, 1000)
        elif cpu_cores <= 2:
            base_config.parallel_append_entries = False
            base_config.batch_size = max(base_config.batch_size // 2, 50)

        # Adjust for memory
        if memory_gb >= 16:
            base_config.log_cache_size = min(base_config.log_cache_size * 3, 100000)
            base_config.snapshot_threshold = min(base_config.snapshot_threshold * 2, 200000)
        elif memory_gb <= 4:
            base_config.log_cache_size = max(base_config.log_cache_size // 2, 1000)
            base_config.snapshot_threshold = max(base_config.snapshot_threshold // 2, 10000)

        return base_config

    @classmethod
    def generate_kubernetes_config(cls, config: RaftPerformanceConfig) -> Dict[str, Any]:
        """Generate Kubernetes ConfigMap for RAFT configuration"""
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "hmssql-raft-config",
                "labels": {
                    "app": "hmssql-cluster"
                }
            },
            "data": {
                "raft-config.yaml": f"""
# RAFT Performance Configuration
election:
  timeout_min: {config.election_timeout_min}
  timeout_max: {config.election_timeout_max}

heartbeat:
  interval: {config.heartbeat_interval}

replication:
  max_entries_per_request: {config.max_log_entries_per_request}
  log_compaction_threshold: {config.log_compaction_threshold}
  parallel_append_entries: {config.parallel_append_entries}

network:
  timeout: {config.network_timeout}
  max_retries: {config.max_retries}
  retry_backoff_base: {config.retry_backoff_base}

batch:
  size: {config.batch_size}
  timeout: {config.batch_timeout}

memory:
  log_cache_size: {config.log_cache_size}
  snapshot_threshold: {config.snapshot_threshold}

performance:
  async_log_writes: {config.async_log_writes}
  compression_enabled: {config.compression_enabled}

monitoring:
  metrics_interval: {config.metrics_collection_interval}
  health_check_interval: {config.health_check_interval}
"""
            }
        }


def optimize_for_environment():
    """CLI tool to generate optimized RAFT configuration"""
    import argparse
    import yaml

    parser = argparse.ArgumentParser(description="Generate optimized RAFT configuration")
    parser.add_argument("--deployment", choices=[e.value for e in DeploymentType],
                       default="production", help="Deployment type")
    parser.add_argument("--workload", choices=[e.value for e in WorkloadType],
                       default="mixed", help="Workload type")
    parser.add_argument("--cluster-size", type=int, default=3,
                       help="Number of nodes in cluster")
    parser.add_argument("--network-latency", type=float, default=1.0,
                       help="Network latency in milliseconds")
    parser.add_argument("--cpu-cores", type=int, default=4,
                       help="CPU cores per node")
    parser.add_argument("--memory-gb", type=int, default=8,
                       help="Memory in GB per node")
    parser.add_argument("--output", choices=["yaml", "json", "k8s"],
                       default="yaml", help="Output format")

    args = parser.parse_args()

    # Get configuration
    if args.cluster_size or args.network_latency != 1.0 or args.cpu_cores != 4 or args.memory_gb != 8:
        # Use auto-tuning
        config = RaftPerformanceTuner.auto_tune(
            args.cluster_size, args.network_latency, args.cpu_cores, args.memory_gb
        )
    else:
        # Use predefined configuration
        deployment_type = DeploymentType(args.deployment)
        workload_type = WorkloadType(args.workload)
        config = RaftPerformanceTuner.get_configuration(deployment_type, workload_type)

    # Output configuration
    if args.output == "yaml":
        config_dict = {
            "election_timeout_min": config.election_timeout_min,
            "election_timeout_max": config.election_timeout_max,
            "heartbeat_interval": config.heartbeat_interval,
            "max_log_entries_per_request": config.max_log_entries_per_request,
            "log_compaction_threshold": config.log_compaction_threshold,
            "network_timeout": config.network_timeout,
            "max_retries": config.max_retries,
            "retry_backoff_base": config.retry_backoff_base,
            "batch_size": config.batch_size,
            "batch_timeout": config.batch_timeout,
            "log_cache_size": config.log_cache_size,
            "snapshot_threshold": config.snapshot_threshold,
            "parallel_append_entries": config.parallel_append_entries,
            "async_log_writes": config.async_log_writes,
            "compression_enabled": config.compression_enabled,
            "metrics_collection_interval": config.metrics_collection_interval,
            "health_check_interval": config.health_check_interval
        }
        print(yaml.dump(config_dict, default_flow_style=False))

    elif args.output == "json":
        import json
        config_dict = config.__dict__
        print(json.dumps(config_dict, indent=2))

    elif args.output == "k8s":
        k8s_config = RaftPerformanceTuner.generate_kubernetes_config(config)
        print(yaml.dump(k8s_config, default_flow_style=False))


if __name__ == "__main__":
    optimize_for_environment()
