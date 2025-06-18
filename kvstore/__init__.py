"""
HyperKV: A CRDT-compliant, Redis-like key-value store
Designed for high performance, distributed operation, and eventual consistency.
"""

__version__ = "1.0.0"
__author__ = "pyHMSSQL Team"

from .core import HyperKVServer, HyperKVConfig, TTLManager, CacheManager, EvictionPolicy
from .crdt import CRDTValue, LWWElementSet, ORSet, VectorClock, HybridLogicalClock
from .raft import RaftNode, RaftConfig
from .storage import StorageEngine, AOFWriter, SnapshotManager
from .pubsub import PubSubManager
from .networking import RedisProtocolHandler, TcpServer

__all__ = [
    'HyperKVServer',
    'HyperKVConfig', 
    'TTLManager',
    'CacheManager',
    'EvictionPolicy',
    'CRDTValue',
    'LWWElementSet',
    'ORSet',
    'VectorClock',
    'HybridLogicalClock',
    'RaftNode',
    'RaftConfig',
    'StorageEngine',
    'AOFWriter',
    'SnapshotManager',
    'PubSubManager',
    'RedisProtocolHandler',
    'TcpServer'
]
