"""
Core configuration for HyperKV Server - Extended configuration class
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
import os
import yaml
from ..config import HyperKVConfig as BaseHyperKVConfig

@dataclass
class HyperKVServerConfig(BaseHyperKVConfig):
    """Extended configuration for HyperKV Server with additional runtime settings"""
    
    # Additional server-specific settings that extend the base config
    worker_threads: int = 4
    enable_profiling: bool = False
    
    # Advanced shard settings
    enable_sharding: bool = True
    num_shards: int = 4
    placement_strategy: str = "NUMA_AWARE"  # NUMA_AWARE, LOAD_BALANCED, LOCALITY_AWARE, ROUND_ROBIN
    enable_consistency: bool = True
    enable_compression: bool = True
    enable_wal: bool = True
    enable_zero_copy: bool = True
    
    def __post_init__(self):
        super().__post_init__() if hasattr(super(), '__post_init__') else None
        
        # Override from environment variables
        self.worker_threads = int(os.getenv("HYPERKV_WORKER_THREADS", self.worker_threads))
        self.enable_profiling = os.getenv("HYPERKV_ENABLE_PROFILING", "false").lower() == "true"
        
        # Map old configuration attributes for backward compatibility
        if not self.node_id:
            self.node_id = "hyperkv-node-1"
    
    # Properties for backward compatibility with the original design
    @property
    def host(self) -> str:
        return self.network.host
    
    @host.setter
    def host(self, value: str):
        self.network.host = value
    
    @property
    def port(self) -> int:
        return self.network.port
    
    @port.setter
    def port(self, value: int):
        self.network.port = value
    
    @property
    def data_dir(self) -> str:
        return self.storage.data_dir
    
    @data_dir.setter
    def data_dir(self, value: str):
        self.storage.data_dir = value
    
    @property
    def storage_backend(self) -> str:
        return self.storage.backend
    
    @storage_backend.setter
    def storage_backend(self, value: str):
        self.storage.backend = value
    
    @property
    def max_memory(self) -> int:
        return self.cache.max_memory
    
    @max_memory.setter
    def max_memory(self, value: int):
        self.cache.max_memory = value
    
    @property
    def max_connections(self) -> int:
        return self.network.max_connections
    
    @max_connections.setter
    def max_connections(self, value: int):
        self.network.max_connections = value
    
    @property
    def eviction_policy(self) -> str:
        return self.cache.eviction_policy
    
    @eviction_policy.setter
    def eviction_policy(self, value: str):
        self.cache.eviction_policy = value
    
    @property
    def enable_clustering(self) -> bool:
        return self.cluster.enabled
    
    @enable_clustering.setter
    def enable_clustering(self, value: bool):
        self.cluster.enabled = value
    
    @property
    def enable_replication(self) -> bool:
        # This would be based on cluster configuration
        return self.cluster.enabled
    
    @property
    def replication_factor(self) -> int:
        # Default replication factor
        return 3
    
    @property
    def ttl_check_interval(self) -> float:
        # Use storage flush interval as TTL check interval
        return 1.0
    
    @property
    def max_keys_per_ttl_check(self) -> int:
        return 100
    
    @property
    def memory_usage_threshold(self) -> float:
        return 0.85
    
    @property
    def eviction_batch_size(self) -> int:
        return self.cache.eviction_batch_size
    
    @property
    def eviction_sample_size(self) -> int:
        return 5
    
    @property
    def enable_metrics(self) -> bool:
        return True
    
    @property
    def metrics_port(self) -> int:
        return 8080
    
    @property
    def enable_tls(self) -> bool:
        return self.network.tls_enabled
    
    @property
    def tls_cert_file(self) -> Optional[str]:
        return self.network.tls_cert_file
    
    @property
    def tls_key_file(self) -> Optional[str]:
        return self.network.tls_key_file
    
    @property
    def auth_password(self) -> Optional[str]:
        return self.security.auth_password
    
    @property
    def aof_enabled(self) -> bool:
        return self.storage.aof_enabled
    
    @property
    def aof_fsync_policy(self) -> str:
        return self.storage.aof_fsync_policy
    
    @property
    def snapshot_enabled(self) -> bool:
        return self.storage.snapshot_enabled
    
    @property
    def cluster_name(self) -> str:
        return "hyperkv-cluster"

# For backward compatibility, alias the new config class
HyperKVConfig = HyperKVServerConfig
