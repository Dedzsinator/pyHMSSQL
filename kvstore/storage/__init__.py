"""
Storage Engine for HyperKV
Supports multiple backends: memory, RocksDB, LMDB
Includes AOF (Append-Only File) and snapshot support
"""

import os
import json
import time
import gzip
import pickle
import threading
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Iterator, Tuple
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

try:
    import rocksdb

    ROCKSDB_AVAILABLE = True
except ImportError:
    ROCKSDB_AVAILABLE = False

try:
    import lmdb

    LMDB_AVAILABLE = True
except ImportError:
    LMDB_AVAILABLE = False


class StorageBackend(ABC):
    """Abstract storage backend interface"""

    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        """Get value by key"""
        pass

    @abstractmethod
    async def set(self, key: str, value: bytes) -> None:
        """Set key-value pair"""
        pass

    @abstractmethod
    async def delete(self, key: str) -> bool:
        """Delete key, return True if existed"""
        pass

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        pass

    @abstractmethod
    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern"""
        pass

    @abstractmethod
    async def scan(
        self, cursor: int = 0, match: str = "*", count: int = 10
    ) -> Tuple[int, List[str]]:
        """Scan keys with cursor"""
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all data"""
        pass

    @abstractmethod
    async def size(self) -> int:
        """Get number of keys"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close storage backend"""
        pass


class MemoryBackend(StorageBackend):
    """In-memory storage backend"""

    def __init__(self):
        self.data: Dict[str, bytes] = {}
        self._lock = threading.RLock()

    async def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            return self.data.get(key)

    async def set(self, key: str, value: bytes) -> None:
        with self._lock:
            self.data[key] = value

    async def delete(self, key: str) -> bool:
        with self._lock:
            if key in self.data:
                del self.data[key]
                return True
            return False

    async def exists(self, key: str) -> bool:
        with self._lock:
            return key in self.data

    async def keys(self, pattern: str = "*") -> List[str]:
        with self._lock:
            if pattern == "*":
                return list(self.data.keys())
            else:
                # Simple pattern matching (glob-style)
                import fnmatch

                return [k for k in self.data.keys() if fnmatch.fnmatch(k, pattern)]

    async def scan(
        self, cursor: int = 0, match: str = "*", count: int = 10
    ) -> Tuple[int, List[str]]:
        with self._lock:
            all_keys = await self.keys(match)
            start = cursor
            end = min(start + count, len(all_keys))
            next_cursor = end if end < len(all_keys) else 0
            return next_cursor, all_keys[start:end]

    async def clear(self) -> None:
        with self._lock:
            self.data.clear()

    async def size(self) -> int:
        with self._lock:
            return len(self.data)

    async def close(self) -> None:
        pass


class RocksDBBackend(StorageBackend):
    """RocksDB storage backend"""

    def __init__(self, db_path: str, options: Dict[str, Any] = None):
        if not ROCKSDB_AVAILABLE:
            raise ImportError("RocksDB is not available")

        self.db_path = db_path
        self.options = options or {}

        # Create directory
        Path(db_path).mkdir(parents=True, exist_ok=True)

        # Set up RocksDB options
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.write_buffer_size = self.options.get("write_buffer_size", 64 * 1024 * 1024)
        opts.max_write_buffer_number = self.options.get("max_write_buffer_number", 3)
        opts.target_file_size_base = self.options.get(
            "target_file_size_base", 64 * 1024 * 1024
        )

        self.db = rocksdb.DB(db_path, opts)
        self._write_opts = rocksdb.WriteOptions()
        self._read_opts = rocksdb.ReadOptions()

    async def get(self, key: str) -> Optional[bytes]:
        value = self.db.get(key.encode("utf-8"), self._read_opts)
        return value

    async def set(self, key: str, value: bytes) -> None:
        self.db.put(self._write_opts, key.encode("utf-8"), value)

    async def delete(self, key: str) -> bool:
        key_bytes = key.encode("utf-8")
        exists = self.db.get(key_bytes, self._read_opts) is not None
        if exists:
            self.db.delete(self._write_opts, key_bytes)
        return exists

    async def exists(self, key: str) -> bool:
        value = self.db.get(key.encode("utf-8"), self._read_opts)
        return value is not None

    async def keys(self, pattern: str = "*") -> List[str]:
        keys = []
        it = self.db.iterkeys()
        it.seek_to_first()

        import fnmatch

        for key in it:
            key_str = key.decode("utf-8")
            if pattern == "*" or fnmatch.fnmatch(key_str, pattern):
                keys.append(key_str)

        return keys

    async def scan(
        self, cursor: int = 0, match: str = "*", count: int = 10
    ) -> Tuple[int, List[str]]:
        all_keys = await self.keys(match)
        start = cursor
        end = min(start + count, len(all_keys))
        next_cursor = end if end < len(all_keys) else 0
        return next_cursor, all_keys[start:end]

    async def clear(self) -> None:
        # Delete all keys
        it = self.db.iterkeys()
        it.seek_to_first()

        batch = rocksdb.WriteBatch()
        for key in it:
            batch.delete(key)

        self.db.write(self._write_opts, batch)

    async def size(self) -> int:
        count = 0
        it = self.db.iterkeys()
        it.seek_to_first()

        for _ in it:
            count += 1

        return count

    async def close(self) -> None:
        if hasattr(self, "db"):
            del self.db


class LMDBBackend(StorageBackend):
    """LMDB storage backend"""

    def __init__(self, db_path: str, options: Dict[str, Any] = None):
        if not LMDB_AVAILABLE:
            raise ImportError("LMDB is not available")

        self.db_path = db_path
        self.options = options or {}

        # Create directory
        Path(db_path).mkdir(parents=True, exist_ok=True)

        # Set up LMDB environment
        map_size = self.options.get("map_size", 1024 * 1024 * 1024)  # 1GB
        self.env = lmdb.open(db_path, map_size=map_size)

    async def get(self, key: str) -> Optional[bytes]:
        with self.env.begin() as txn:
            value = txn.get(key.encode("utf-8"))
            return value

    async def set(self, key: str, value: bytes) -> None:
        with self.env.begin(write=True) as txn:
            txn.put(key.encode("utf-8"), value)

    async def delete(self, key: str) -> bool:
        with self.env.begin(write=True) as txn:
            key_bytes = key.encode("utf-8")
            exists = txn.get(key_bytes) is not None
            if exists:
                txn.delete(key_bytes)
            return exists

    async def exists(self, key: str) -> bool:
        with self.env.begin() as txn:
            return txn.get(key.encode("utf-8")) is not None

    async def keys(self, pattern: str = "*") -> List[str]:
        keys = []
        with self.env.begin() as txn:
            cursor = txn.cursor()
            import fnmatch

            for key, _ in cursor:
                key_str = key.decode("utf-8")
                if pattern == "*" or fnmatch.fnmatch(key_str, pattern):
                    keys.append(key_str)

        return keys

    async def scan(
        self, cursor: int = 0, match: str = "*", count: int = 10
    ) -> Tuple[int, List[str]]:
        all_keys = await self.keys(match)
        start = cursor
        end = min(start + count, len(all_keys))
        next_cursor = end if end < len(all_keys) else 0
        return next_cursor, all_keys[start:end]

    async def clear(self) -> None:
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor()
            cursor.first()

            keys_to_delete = []
            for key, _ in cursor:
                keys_to_delete.append(key)

            for key in keys_to_delete:
                txn.delete(key)

    async def size(self) -> int:
        with self.env.begin() as txn:
            return txn.stat()["entries"]

    async def close(self) -> None:
        if hasattr(self, "env"):
            self.env.close()


class AOFWriter:
    """Append-Only File writer for persistence"""

    def __init__(self, file_path: str, fsync_policy: str = "everysec"):
        self.file_path = Path(file_path)
        self.fsync_policy = fsync_policy  # always, everysec, no
        self.file_handle = None
        self._lock = threading.Lock()
        self._last_fsync = time.time()
        self._pending_writes = 0
        self._background_task = None

        # Create directory
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def start_background_fsync(self):
        """Start background fsync task - should be called when event loop is available"""
        if self.fsync_policy == "everysec" and self._background_task is None:
            try:
                self._background_task = asyncio.create_task(self._background_fsync())
            except RuntimeError:
                # No event loop running yet, will be started later
                pass

    def open(self):
        """Open AOF file for writing"""
        with self._lock:
            if self.file_handle is None:
                self.file_handle = open(self.file_path, "ab")

    def close(self):
        """Close AOF file"""
        with self._lock:
            if self.file_handle:
                self.file_handle.close()
                self.file_handle = None

    async def write_command(self, command: Dict[str, Any]):
        """Write command to AOF"""
        if not self.file_handle:
            self.open()

        # Format: timestamp|command_json\n
        entry = f"{time.time()}|{json.dumps(command)}\n"

        with self._lock:
            self.file_handle.write(entry.encode("utf-8"))
            self._pending_writes += 1

            if self.fsync_policy == "always":
                self.file_handle.flush()
                os.fsync(self.file_handle.fileno())
                self._pending_writes = 0

    async def _background_fsync(self):
        """Background task for periodic fsync"""
        while True:
            await asyncio.sleep(1.0)  # Every second

            with self._lock:
                if self.file_handle and self._pending_writes > 0:
                    self.file_handle.flush()
                    os.fsync(self.file_handle.fileno())
                    self._pending_writes = 0
                    self._last_fsync = time.time()

    def replay(self) -> Iterator[Dict[str, Any]]:
        """Replay commands from AOF file"""
        if not self.file_path.exists():
            return

        with open(self.file_path, "rb") as f:
            for line in f:
                try:
                    line = line.decode("utf-8").strip()
                    if "|" in line:
                        timestamp_str, command_json = line.split("|", 1)
                        command = json.loads(command_json)
                        yield command
                except Exception as e:
                    logger.error(f"Error replaying AOF line: {e}")

    def rewrite(self, commands: List[Dict[str, Any]]):
        """Rewrite AOF file with compacted commands"""
        temp_path = self.file_path.with_suffix(".tmp")

        try:
            with open(temp_path, "wb") as f:
                for command in commands:
                    entry = f"{time.time()}|{json.dumps(command)}\n"
                    f.write(entry.encode("utf-8"))
                f.flush()
                os.fsync(f.fileno())

            # Atomic replace
            temp_path.replace(self.file_path)

        except Exception as e:
            logger.error(f"Error rewriting AOF: {e}")
            if temp_path.exists():
                temp_path.unlink()


class SnapshotManager:
    """Manages database snapshots"""

    def __init__(self, snapshot_dir: str, compression: bool = True):
        self.snapshot_dir = Path(snapshot_dir)
        self.compression = compression
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    async def create_snapshot(
        self, data: Dict[str, Any], metadata: Dict[str, Any] = None
    ) -> str:
        """Create a snapshot"""
        timestamp = int(time.time())
        snapshot_id = f"snapshot_{timestamp}"

        snapshot_data = {
            "timestamp": timestamp,
            "data": data,
            "metadata": metadata or {},
        }

        snapshot_path = self.snapshot_dir / f"{snapshot_id}.pkl"

        if self.compression:
            snapshot_path = snapshot_path.with_suffix(".pkl.gz")
            with gzip.open(snapshot_path, "wb") as f:
                pickle.dump(snapshot_data, f)
        else:
            with open(snapshot_path, "wb") as f:
                pickle.dump(snapshot_data, f)

        logger.info(f"Created snapshot: {snapshot_id}")
        return snapshot_id

    async def load_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """Load a snapshot"""
        snapshot_path = self.snapshot_dir / f"{snapshot_id}.pkl"
        gz_path = snapshot_path.with_suffix(".pkl.gz")

        try:
            if gz_path.exists():
                with gzip.open(gz_path, "rb") as f:
                    return pickle.load(f)
            elif snapshot_path.exists():
                with open(snapshot_path, "rb") as f:
                    return pickle.load(f)
        except Exception as e:
            logger.error(f"Error loading snapshot {snapshot_id}: {e}")

        return None

    def list_snapshots(self) -> List[str]:
        """List available snapshots"""
        snapshots = []
        for path in self.snapshot_dir.glob("snapshot_*.pkl*"):
            snapshot_id = path.stem
            if snapshot_id.endswith(".pkl"):
                snapshot_id = snapshot_id[:-4]
            snapshots.append(snapshot_id)

        return sorted(snapshots)

    def get_latest_snapshot(self) -> Optional[str]:
        """Get the latest snapshot ID"""
        snapshots = self.list_snapshots()
        return snapshots[-1] if snapshots else None

    async def cleanup_old_snapshots(self, keep_count: int = 5):
        """Remove old snapshots, keeping only the most recent ones"""
        snapshots = self.list_snapshots()

        if len(snapshots) > keep_count:
            to_remove = snapshots[:-keep_count]

            for snapshot_id in to_remove:
                snapshot_path = self.snapshot_dir / f"{snapshot_id}.pkl"
                gz_path = snapshot_path.with_suffix(".pkl.gz")

                if gz_path.exists():
                    gz_path.unlink()
                elif snapshot_path.exists():
                    snapshot_path.unlink()

                logger.info(f"Removed old snapshot: {snapshot_id}")

    async def load_latest(self) -> Optional[Dict[str, Any]]:
        """Load the latest snapshot"""
        latest_snapshot_id = self.get_latest_snapshot()
        if latest_snapshot_id:
            snapshot_data = await self.load_snapshot(latest_snapshot_id)
            if snapshot_data:
                return snapshot_data.get("data", {})
        return None


class StorageEngine:
    """Main storage engine combining backend, AOF, and snapshots"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.backend = self._create_backend()

        # AOF
        self.aof_writer = None
        if config.get("aof_enabled", True):
            aof_path = Path(config["data_dir"]) / "appendonly.aof"
            self.aof_writer = AOFWriter(
                str(aof_path), config.get("aof_fsync_policy", "everysec")
            )

        # Snapshots
        self.snapshot_manager = None
        if config.get("snapshot_enabled", True):
            snapshot_dir = Path(config["data_dir"]) / "snapshots"
            self.snapshot_manager = SnapshotManager(
                str(snapshot_dir), config.get("snapshot_compression", True)
            )

        # Metrics
        self.stats = {
            "commands_processed": 0,
            "keys_expired": 0,
            "memory_usage": 0,
            "aof_rewrites": 0,
            "snapshots_created": 0,
        }

        self._lock = threading.RLock()

    def _create_backend(self) -> StorageBackend:
        """Create storage backend based on configuration"""
        backend_type = self.config.get("backend", "memory")

        if backend_type == "memory":
            return MemoryBackend()
        elif backend_type == "rocksdb":
            db_path = Path(self.config["data_dir"]) / "rocksdb"
            return RocksDBBackend(str(db_path), self.config.get("rocksdb_options", {}))
        elif backend_type == "lmdb":
            db_path = Path(self.config["data_dir"]) / "lmdb"
            return LMDBBackend(str(db_path), self.config.get("lmdb_options", {}))
        else:
            raise ValueError(f"Unknown backend type: {backend_type}")

    async def initialize(self):
        """Initialize storage engine"""
        # Start AOF background task if needed
        if self.aof_writer:
            self.aof_writer.start_background_fsync()

        # Restore from snapshot if available
        if self.snapshot_manager:
            latest_snapshot = self.snapshot_manager.get_latest_snapshot()
            if latest_snapshot:
                snapshot_data = await self.snapshot_manager.load_snapshot(
                    latest_snapshot
                )
                if snapshot_data:
                    await self._restore_from_snapshot(snapshot_data)
                    logger.info(f"Restored from snapshot: {latest_snapshot}")

        # Replay AOF
        if self.aof_writer:
            await self._replay_aof()

    async def _restore_from_snapshot(self, snapshot_data: Dict[str, Any]):
        """Restore data from snapshot"""
        data = snapshot_data.get("data", {})

        for key, value in data.items():
            if isinstance(value, bytes):
                await self.backend.set(key, value)
            else:
                # Convert to bytes if needed
                await self.backend.set(key, json.dumps(value).encode("utf-8"))

    async def _replay_aof(self):
        """Replay commands from AOF"""
        command_count = 0

        for command in self.aof_writer.replay():
            await self._execute_command(command, from_aof=True)
            command_count += 1

        if command_count > 0:
            logger.info(f"Replayed {command_count} commands from AOF")

    async def _execute_command(self, command: Dict[str, Any], from_aof: bool = False):
        """Execute a command"""
        cmd_type = command.get("type")

        if cmd_type == "SET":
            key = command["key"]
            value = command["value"]
            if isinstance(value, str):
                value = value.encode("utf-8")
            await self.backend.set(key, value)

        elif cmd_type == "DEL":
            key = command["key"]
            await self.backend.delete(key)

        elif cmd_type == "CLEAR":
            await self.backend.clear()

        # Update stats
        if not from_aof:
            with self._lock:
                self.stats["commands_processed"] += 1

    # Public interface

    async def get(self, key: str) -> Optional[bytes]:
        """Get value by key"""
        return await self.backend.get(key)

    async def set(self, key: str, value: bytes) -> None:
        """Set key-value pair"""
        await self.backend.set(key, value)

        # Log to AOF
        if self.aof_writer:
            command = {
                "type": "SET",
                "key": key,
                "value": value.decode("utf-8", errors="ignore"),
            }
            await self.aof_writer.write_command(command)

        with self._lock:
            self.stats["commands_processed"] += 1

    async def delete(self, key: str) -> bool:
        """Delete key"""
        result = await self.backend.delete(key)

        # Log to AOF
        if self.aof_writer and result:
            command = {"type": "DEL", "key": key}
            await self.aof_writer.write_command(command)

        with self._lock:
            self.stats["commands_processed"] += 1

        return result

    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        return await self.backend.exists(key)

    async def keys(self, pattern: str = "*") -> List[str]:
        """Get keys matching pattern"""
        return await self.backend.keys(pattern)

    async def scan(
        self, cursor: int = 0, match: str = "*", count: int = 10
    ) -> Tuple[int, List[str]]:
        """Scan keys with cursor"""
        return await self.backend.scan(cursor, match, count)

    async def clear(self) -> None:
        """Clear all data"""
        await self.backend.clear()

        # Log to AOF
        if self.aof_writer:
            command = {"type": "CLEAR"}
            await self.aof_writer.write_command(command)

        with self._lock:
            self.stats["commands_processed"] += 1

    async def size(self) -> int:
        """Get number of keys"""
        return await self.backend.size()

    async def create_snapshot(self) -> Optional[str]:
        """Create a snapshot of current data"""
        if not self.snapshot_manager:
            return None

        # Get all data
        keys = await self.backend.keys()
        data = {}

        for key in keys:
            value = await self.backend.get(key)
            if value:
                data[key] = value

        metadata = {"stats": self.stats.copy(), "config": self.config}

        snapshot_id = await self.snapshot_manager.create_snapshot(data, metadata)

        with self._lock:
            self.stats["snapshots_created"] += 1

        return snapshot_id

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        with self._lock:
            return self.stats.copy()

    async def close(self):
        """Close storage engine"""
        if self.aof_writer:
            self.aof_writer.close()

        await self.backend.close()
