"""
Configuration management for HyperKV
"""

import os
import yaml
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class RaftConfig:
    """Raft consensus configuration"""
    election_timeout_min: float = 0.15
    election_timeout_max: float = 0.3
    heartbeat_interval: float = 0.05
    log_compaction_threshold: int = 1000
    max_log_entries_per_request: int = 100
    snapshot_interval: int = 10000
    
    
@dataclass
class StorageConfig:
    """Storage engine configuration"""
    data_dir: str = "./data/kvstore"
    aof_enabled: bool = True
    aof_fsync_policy: str = "everysec"  # always, everysec, no
    aof_rewrite_percentage: int = 100
    aof_rewrite_min_size: int = 64 * 1024 * 1024  # 64MB
    
    snapshot_enabled: bool = True
    snapshot_interval: int = 300  # seconds
    snapshot_compression: bool = True
    
    # Backend: memory, rocksdb, lmdb
    backend: str = "memory"
    rocksdb_options: Dict[str, Any] = field(default_factory=dict)
    lmdb_options: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class NetworkConfig:
    """Network configuration"""
    host: str = "0.0.0.0"
    port: int = 6379
    max_connections: int = 10000
    tcp_nodelay: bool = True
    tcp_keepalive: bool = True
    
    # TLS configuration
    tls_enabled: bool = False
    tls_cert_file: Optional[str] = None
    tls_key_file: Optional[str] = None
    tls_ca_file: Optional[str] = None
    
    # Protocol settings
    protocol: str = "resp"  # resp or grpc
    grpc_port: int = 6380
    

@dataclass
class CRDTConfig:
    """CRDT configuration"""
    clock_type: str = "hlc"  # vector, hlc, lamport
    conflict_resolution: str = "lww"  # lww, max, min
    gc_interval: int = 3600  # garbage collection interval
    max_vector_size: int = 1000
    

@dataclass
class CacheConfig:
    """Cache configuration"""
    max_memory: int = 1024 * 1024 * 1024  # 1GB
    eviction_policy: str = "lru"  # lru, arc, random, volatile-lru
    eviction_batch_size: int = 100
    

@dataclass
class PubSubConfig:
    """Pub/Sub configuration"""
    max_channels: int = 100000
    max_subscribers_per_channel: int = 1000
    message_buffer_size: int = 1000
    

@dataclass 
class SecurityConfig:
    """Security configuration"""
    require_auth: bool = False
    auth_password: Optional[str] = None
    acl_enabled: bool = False
    acl_file: Optional[str] = None
    

@dataclass
class ClusterConfig:
    """Cluster configuration"""
    enabled: bool = False
    node_id: str = ""
    peers: List[str] = field(default_factory=list)
    service_discovery: str = "static"  # static, k8s, consul
    k8s_namespace: str = "default"
    k8s_service_name: str = "hyperkv"
    

@dataclass
class HyperKVConfig:
    """Main HyperKV configuration"""
    # Core settings
    node_id: str = ""
    log_level: str = "INFO"
    pid_file: Optional[str] = None
    daemonize: bool = False
    
    # Component configurations
    raft: RaftConfig = field(default_factory=RaftConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    network: NetworkConfig = field(default_factory=NetworkConfig)
    crdt: CRDTConfig = field(default_factory=CRDTConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    pubsub: PubSubConfig = field(default_factory=PubSubConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    cluster: ClusterConfig = field(default_factory=ClusterConfig)
    
    @classmethod
    def from_file(cls, config_path: str) -> 'HyperKVConfig':
        """Load configuration from YAML file"""
        config_path = Path(config_path)
        if not config_path.exists():
            return cls()
            
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
            
        return cls.from_dict(data)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HyperKVConfig':
        """Create config from dictionary"""
        config = cls()
        
        for key, value in data.items():
            if hasattr(config, key):
                attr = getattr(config, key)
                if hasattr(attr, '__dict__'):  # It's a dataclass
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            if hasattr(attr, sub_key):
                                setattr(attr, sub_key, sub_value)
                else:
                    setattr(config, key, value)
                    
        return config
    
    @classmethod
    def from_env(cls) -> 'HyperKVConfig':
        """Load configuration from environment variables"""
        config = cls()
        
        # Node settings
        config.node_id = os.getenv('HYPERKV_NODE_ID', config.node_id)
        config.log_level = os.getenv('HYPERKV_LOG_LEVEL', config.log_level)
        
        # Network settings
        config.network.host = os.getenv('HYPERKV_HOST', config.network.host)
        config.network.port = int(os.getenv('HYPERKV_PORT', config.network.port))
        
        # Storage settings
        config.storage.data_dir = os.getenv('HYPERKV_DATA_DIR', config.storage.data_dir)
        config.storage.backend = os.getenv('HYPERKV_STORAGE_BACKEND', config.storage.backend)
        
        # Cluster settings
        config.cluster.enabled = os.getenv('HYPERKV_CLUSTER_ENABLED', 'false').lower() == 'true'
        peers_env = os.getenv('HYPERKV_CLUSTER_PEERS', '')
        if peers_env:
            config.cluster.peers = [p.strip() for p in peers_env.split(',')]
            
        # Kubernetes settings
        config.cluster.k8s_namespace = os.getenv('HYPERKV_K8S_NAMESPACE', config.cluster.k8s_namespace)
        config.cluster.k8s_service_name = os.getenv('HYPERKV_K8S_SERVICE_NAME', config.cluster.k8s_service_name)
        
        return config
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary"""
        result = {}
        for key, value in self.__dict__.items():
            if hasattr(value, '__dict__'):  # It's a dataclass
                result[key] = {k: v for k, v in value.__dict__.items()}
            else:
                result[key] = value
        return result
    
    def save_to_file(self, config_path: str):
        """Save configuration to YAML file"""
        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(config_path, 'w') as f:
            yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)
            
    def validate(self) -> List[str]:
        """Validate configuration and return any errors"""
        errors = []
        
        if self.cluster.enabled and not self.node_id:
            errors.append("node_id is required when clustering is enabled")
            
        if self.network.tls_enabled:
            if not self.network.tls_cert_file:
                errors.append("tls_cert_file is required when TLS is enabled")
            if not self.network.tls_key_file:
                errors.append("tls_key_file is required when TLS is enabled")
                
        if self.security.require_auth and not self.security.auth_password:
            errors.append("auth_password is required when authentication is enabled")
            
        if self.storage.backend not in ['memory', 'rocksdb', 'lmdb']:
            errors.append(f"Invalid storage backend: {self.storage.backend}")
            
        if self.crdt.clock_type not in ['vector', 'hlc', 'lamport']:
            errors.append(f"Invalid clock type: {self.crdt.clock_type}")
            
        return errors
