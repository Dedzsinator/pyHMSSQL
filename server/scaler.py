"""Replication manager for primary-replica architecture.

This module handles data replication across multiple database instances
for improved read scalability and fault tolerance.
"""
import logging
import threading
import time
import queue
import json
import socket
import uuid
from enum import Enum

class ReplicaRole(Enum):
    """Roles in the replication system"""
    PRIMARY = 1
    REPLICA = 2
    CANDIDATE = 3  # For leader election

class ReplicationMode(Enum):
    """Replication modes for different consistency guarantees"""
    SYNC = 1         # Fully synchronous - wait for all replicas
    SEMI_SYNC = 2    # Semi-synchronous - wait for some replicas
    ASYNC = 3        # Asynchronous - don't wait for replicas

class ReplicationManager:
    """Manages primary-replica replication for horizontal scaling"""

    def __init__(self, server_id=None, mode=ReplicationMode.SEMI_SYNC, sync_replicas=1):
        """Initialize the replication manager.

        Args:
            server_id: Unique ID for this server instance
            mode: Replication mode (SYNC, SEMI_SYNC, ASYNC)
            sync_replicas: Number of replicas to wait for in SEMI_SYNC mode
        """
        self.server_id = server_id or str(uuid.uuid4())
        self.mode = mode
        self.sync_replicas = sync_replicas
        self.role = ReplicaRole.PRIMARY  # Default to primary until configured
        self.primary_id = None  # ID of the primary server (if we're a replica)
        self.replicas = {}  # {replica_id: {host, port, last_seen, lag}}
        self.oplog = []  # Operation log for replication
        self.oplog_position = 0  # Current position in oplog
        self.replication_queue = queue.Queue()  # Queue for async replication
        self.lock = threading.RLock()  # Reentrant lock for thread safety
        self.logger = logging.getLogger("replication")
        self.catalog_manager = None  # Placeholder for catalog manager
        self.execution_engine = None  # Placeholder for execution engine
        self.sql_parser = None  # Placeholder for SQL parser

        # Start background workers
        self.running = True
        self.worker_thread = threading.Thread(target=self._replication_worker, daemon=True)
        self.worker_thread.start()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_worker, daemon=True)
        self.heartbeat_thread.start()

    def register_as_replica(self, primary_host, primary_port):
        """Register this server as a replica to a primary.

        Args:
            primary_host: Hostname of the primary server
            primary_port: Port of the primary server

        Returns:
            bool: True if successful
        """
        try:
            # Connect to primary and register
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((primary_host, primary_port))

            # Send registration message
            message = {
                "type": "replica_register",
                "server_id": self.server_id,
                "host": socket.gethostname(),
                "port": 9999  # Assuming standard port
            }
            sock.sendall(json.dumps(message).encode('utf-8'))

            # Get response
            response = sock.recv(4096)
            response_data = json.loads(response.decode('utf-8'))
            sock.close()

            if response_data.get("status") == "success":
                with self.lock:
                    self.role = ReplicaRole.REPLICA
                    self.primary_id = response_data.get("primary_id")
                    self.oplog_position = response_data.get("oplog_position", 0)

                self.logger.info(f"Registered as replica to primary {self.primary_id}")
                return True
            else:
                self.logger.error(f"Failed to register as replica: {response_data.get('error')}")
                return False

        except RuntimeError as e:
            self.logger.error(f"Error registering as replica: {str(e)}")
            return False

    def register_replica(self, replica_id, host, port):
        """Register a new replica to this primary server.

        Args:
            replica_id: Unique ID of the replica
            host: Hostname of the replica
            port: Port of the replica

        Returns:
            dict: Registration response with oplog position
        """
        with self.lock:
            if self.role != ReplicaRole.PRIMARY:
                return {"status": "error", "error": "This server is not a primary"}

            self.replicas[replica_id] = {
                "host": host,
                "port": port,
                "last_seen": time.time(),
                "lag": 0,
                "status": "online"
            }

            self.logger.info(f"Registered new replica: {replica_id} at {host}:{port}")
            return {
                "status": "success",
                "primary_id": self.server_id,
                "oplog_position": len(self.oplog)
            }

    def log_operation(self, operation):
        """Log an operation to the oplog for replication.

        Args:
            operation: Dictionary with operation details

        Returns:
            bool: True if operation was successfully logged and replicated
        """
        with self.lock:
            if self.role != ReplicaRole.PRIMARY:
                self.logger.warning("Cannot log operation: not a primary")
                return False

            # Add timestamp and server_id to operation
            operation["timestamp"] = time.time()
            operation["server_id"] = self.server_id
            operation["oplog_id"] = len(self.oplog)

            # Add to oplog
            self.oplog.append(operation)

            # Replicate based on mode
            if self.mode == ReplicationMode.SYNC:
                return self._sync_replicate(operation)
            elif self.mode == ReplicationMode.SEMI_SYNC:
                return self._semi_sync_replicate(operation, self.sync_replicas)
            else:  # ASYNC
                self.replication_queue.put(operation)
                return True

    def _sync_replicate(self, operation):
        """Synchronously replicate an operation to all replicas.

        Args:
            operation: The operation to replicate

        Returns:
            bool: True if all replicas acknowledged
        """
        if not self.replicas:
            return True  # No replicas to sync with

        success_count = 0
        for replica_id, replica in self.replicas.items():
            if replica["status"] != "online":
                continue

            if self._send_operation_to_replica(replica_id, replica, operation):
                success_count += 1

        return success_count == len(self.replicas)

    def _semi_sync_replicate(self, operation, min_replicas):
        """Semi-synchronously replicate an operation.

        Args:
            operation: The operation to replicate
            min_replicas: Minimum number of replicas that must acknowledge

        Returns:
            bool: True if enough replicas acknowledged
        """
        if len(self.replicas) < min_replicas:
            self.logger.warning(f"Not enough replicas: have {len(self.replicas)}, need {min_replicas}")
            return len(self.replicas) == 0  # True if no replicas needed

        success_count = 0
        for replica_id, replica in self.replicas.items():
            if replica["status"] != "online":
                continue

            if self._send_operation_to_replica(replica_id, replica, operation):
                success_count += 1
                if success_count >= min_replicas:
                    break

        result = success_count >= min_replicas
        if not result:
            self.logger.warning(f"Semi-sync replication failed: only {success_count}/{min_replicas} acknowledged")
        return result

    def _send_operation_to_replica(self, replica_id, replica, operation):
        """Send an operation to a specific replica.

        Args:
            replica_id: ID of the replica
            replica: Replica information dictionary
            operation: The operation to send

        Returns:
            bool: True if acknowledged successfully
        """
        try:
            # Connect to replica
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)  # 5 second timeout
            sock.connect((replica["host"], replica["port"]))

            # Send operation
            message = {
                "type": "replicate_op",
                "operation": operation,
                "primary_id": self.server_id
            }
            sock.sendall(json.dumps(message).encode('utf-8'))

            # Get acknowledgement
            response = sock.recv(4096)
            response_data = json.loads(response.decode('utf-8'))
            sock.close()

            if response_data.get("status") == "success":
                # Update replica status
                with self.lock:
                    replica["last_seen"] = time.time()
                    replica["lag"] = len(self.oplog) - response_data.get("oplog_position", 0)
                return True
            else:
                self.logger.error(f"Replica {replica_id} error: {response_data.get('error')}")
                return False

        except RuntimeError as e:
            self.logger.error(f"Error sending to replica {replica_id}: {str(e)}")
            # Mark replica as potentially offline
            with self.lock:
                if time.time() - replica["last_seen"] > 30:  # 30 second threshold
                    replica["status"] = "offline"
            return False

    def apply_operation(self, operation):
        """Apply a replicated operation locally (for replicas).

        Args:
            operation: The operation to apply

        Returns:
            bool: True if operation was applied successfully
        """
        if self.role != ReplicaRole.REPLICA:
            self.logger.warning("Cannot apply operation: not a replica")
            return False

        # Check if operation is already applied or out of order
        if operation["oplog_id"] < self.oplog_position:
            # Already applied
            return True
        elif operation["oplog_id"] > self.oplog_position:
            # Out of order, store for later
            self.logger.warning(f"Received out-of-order operation: {operation['oplog_id']}, current position: {self.oplog_position}")
            return False

        # Apply the operation based on its type
        op_type = operation.get("type")

        # Apply the operation to the local database
        try:
            if not hasattr(self, 'catalog_manager'):
                self.logger.error("Cannot apply operation: catalog_manager not set")
                return False

            if op_type == "INSERT":
                table_name = operation.get("table")
                # Either use the record directly or extract from values/columns
                record = operation.get("record")
                if not record and operation.get("values") and operation.get("columns"):
                    # Convert values/columns to record format
                    columns = operation.get("columns", [])
                    values = operation.get("values", [[]])
                    if values and len(values) > 0:
                        record = dict(zip(columns, values[0]))

                if table_name and record:
                    self.catalog_manager.insert_record(table_name, record)
                else:
                    self.logger.warning("Incomplete INSERT operation data")

            elif op_type == "UPDATE":
                table_name = operation.get("table")
                record_id = operation.get("record_id")
                updates = operation.get("updates", {})
                condition = operation.get("condition")

                if table_name and (record_id or condition):
                    if record_id:
                        # Update by ID
                        self.catalog_manager.update_record_by_id(table_name, record_id, updates)
                    elif condition:
                        # Update by condition
                        self.catalog_manager.update_records(table_name, condition, updates)
                else:
                    self.logger.warning("Incomplete UPDATE operation data")

            elif op_type == "DELETE":
                table_name = operation.get("table")
                record_id = operation.get("record_id")
                condition = operation.get("condition")

                if table_name and (record_id or condition):
                    if record_id:
                        # Delete by ID
                        self.catalog_manager.delete_record_by_id(table_name, record_id)
                    elif condition:
                        # Delete by condition
                        self.catalog_manager.delete_records(table_name, condition)
                else:
                    self.logger.warning("Incomplete DELETE operation data")

            elif op_type in ("CREATE_TABLE", "DROP_TABLE", "CREATE_INDEX", "DROP_INDEX"):
                # For schema operations, we typically need to execute the original SQL
                # to ensure all side effects are properly applied
                query = operation.get("query")
                if query:
                    # Use SQL parser and execution engine to process the query directly
                    self.execution_engine.execute(self.sql_parser.parse_sql(query))
                else:
                    self.logger.warning(f"No query provided for {op_type} operation")

            # Advance oplog position
            self.oplog_position += 1
            return True

        except RuntimeError as e:
            self.logger.error(f"Error applying operation: {str(e)}")
            return False

    def _replication_worker(self):
        """Background worker thread for asynchronous replication."""
        while self.running:
            try:
                # Get an operation from the queue
                try:
                    operation = self.replication_queue.get(timeout=1)
                except queue.Empty:
                    continue

                # Try to replicate to all online replicas
                with self.lock:
                    for replica_id, replica in self.replicas.items():
                        if replica["status"] == "online":
                            self._send_operation_to_replica(replica_id, replica, operation)

                self.replication_queue.task_done()
            except RuntimeError as e:
                self.logger.error(f"Error in replication worker: {str(e)}")

    def _heartbeat_worker(self):
        """Background worker thread for replica heartbeats."""
        while self.running:
            try:
                if self.role == ReplicaRole.PRIMARY:
                    # Primary sends heartbeats to replicas
                    with self.lock:
                        for replica_id, replica in list(self.replicas.items()):
                            try:
                                # Send heartbeat
                                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                sock.settimeout(3)
                                sock.connect((replica["host"], replica["port"]))

                                message = {
                                    "type": "heartbeat",
                                    "primary_id": self.server_id,
                                    "timestamp": time.time()
                                }
                                sock.sendall(json.dumps(message).encode('utf-8'))

                                # Get response
                                response = sock.recv(4096)
                                response_data = json.loads(response.decode('utf-8'))
                                sock.close()

                                if response_data.get("status") == "success":
                                    replica["last_seen"] = time.time()
                                    replica["lag"] = len(self.oplog) - response_data.get("oplog_position", 0)
                                    replica["status"] = "online"

                            except RuntimeError as e:
                                self.logger.warning(f"Heartbeat to replica {replica_id} failed: {str(e)}")
                                if time.time() - replica["last_seen"] > 30:
                                    replica["status"] = "offline"

                elif self.role == ReplicaRole.REPLICA:
                    # Replica sends heartbeat to primary to report status
                    if self.primary_id:
                        # Implement primary heartbeat logic
                        pass

                # Wait before next heartbeat cycle
                time.sleep(5)

            except RuntimeError as e:
                self.logger.error(f"Error in heartbeat worker: {str(e)}")
                time.sleep(5)  # Wait before retrying

    def stop(self):
        """Stop the replication manager."""
        self.running = False
        self.worker_thread.join(timeout=5)
        self.heartbeat_thread.join(timeout=5)
