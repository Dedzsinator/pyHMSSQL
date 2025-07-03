@dataclass
class ShardConfig:
    """Configuration for a single shard"""

    shard_id: int
    cpu_core: int = 0
    numa_node: int = 0
    memory_limit: int = 64 * 1024 * 1024  # 64MB default
    wal_enabled: bool = True
    compression_enabled: bool = True
    cache_size: int = 1024
    enable_zero_copy: bool = False
