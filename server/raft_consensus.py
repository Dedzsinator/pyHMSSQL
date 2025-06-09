"""RAFT Consensus Algorithm Implementation for Database Replication.

This module implements the RAFT consensus algorithm for leader election and
log replication in a distributed database cluster. Provides fault tolerance
and automatic failover similar to GitHub's Orchestrator.
"""
import json
import logging
import random
import socket
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any, Callable
import hashlib


class NodeState(Enum):
    """RAFT node states"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class MessageType(Enum):
    """RAFT message types"""
    VOTE_REQUEST = "vote_request"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_RESPONSE = "append_response"
    INSTALL_SNAPSHOT = "install_snapshot"
    SNAPSHOT_RESPONSE = "snapshot_response"


@dataclass
class LogEntry:
    """A single log entry in the RAFT log"""
    term: int
    index: int
    operation: Dict[str, Any]
    committed: bool = False
    timestamp: float = field(default_factory=time.time)
    checksum: str = field(default="")

    def __post_init__(self):
        if not self.checksum:
            # Create checksum for integrity verification
            data = f"{self.term}{self.index}{json.dumps(self.operation, sort_keys=True)}"
            self.checksum = hashlib.sha256(data.encode()).hexdigest()[:16]


@dataclass
class VoteRequest:
    """RAFT vote request message"""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    """RAFT vote response message"""
    term: int
    vote_granted: bool
    voter_id: str


@dataclass
class AppendEntries:
    """RAFT append entries (heartbeat/log replication) message"""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendResponse:
    """RAFT append entries response message"""
    term: int
    success: bool
    follower_id: str
    match_index: int = 0
    conflict_index: int = 0


class ClusterMembership:
    """Manages cluster membership and configuration changes"""
    
    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.configuration_index = 0
        self.pending_config = None
        self.lock = threading.RLock()
    
    def add_node(self, node_id: str, host: str, port: int, role: str = "voter") -> bool:
        """Add a node to the cluster"""
        with self.lock:
            if node_id not in self.nodes:
                self.nodes[node_id] = {
                    "host": host,
                    "port": port,
                    "role": role,  # voter, learner, or removed
                    "last_seen": time.time(),
                    "status": "online"
                }
                self.configuration_index += 1
                return True
            return False
    
    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the cluster"""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id]["role"] = "removed"
                self.configuration_index += 1
                return True
            return False
    
    def get_voting_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get all nodes that can vote"""
        with self.lock:
            return {
                node_id: info for node_id, info in self.nodes.items()
                if info["role"] == "voter" and info["status"] == "online"
            }
    
    def get_all_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get all nodes in the cluster"""
        with self.lock:
            return self.nodes.copy()


class RaftNode:
    """A single node in the RAFT cluster"""
    
    def __init__(self, node_id: str, host: str, port: int, 
                 peers: List[Tuple[str, str, int]] = None,
                 election_timeout_min: float = 5.0,
                 election_timeout_max: float = 10.0,
                 heartbeat_interval: float = 2.0):
        """Initialize a RAFT node.
        
        Args:
            node_id: Unique identifier for this node
            host: Host address for this node
            port: Port for this node
            peers: List of (node_id, host, port) tuples for other nodes
            election_timeout_min: Minimum election timeout in seconds
            election_timeout_max: Maximum election timeout in seconds
            heartbeat_interval: Heartbeat interval for leader
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        
        # RAFT state
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Election state
        self.votes_received = 0
        self.current_election_term = 0
        
        # Cluster membership
        self.cluster = ClusterMembership()
        
        # Add initial peers
        if peers:
            for peer_id, peer_host, peer_port in peers:
                self.cluster.add_node(peer_id, peer_host, peer_port)
        
        # Add self to cluster
        self.cluster.add_node(node_id, host, port)
        
        # Timing
        self.last_heartbeat = time.time()
        self.election_deadline = self._reset_election_timeout()
        
        # Threading and logging
        self.running = True
        self.lock = threading.RLock()
        self.logger = logging.getLogger(f"raft.{node_id}")
        
        # Network
        self.server_socket = None
        self.server_thread = None
        
        # Callbacks for state machine
        self.apply_callback: Optional[Callable[[LogEntry], Any]] = None
        self.leadership_callback: Optional[Callable[[bool], None]] = None
        
        # Metrics and monitoring
        self.metrics = {
            "elections_started": 0,
            "elections_won": 0,
            "log_entries_received": 0,
            "log_entries_applied": 0,
            "heartbeats_sent": 0,
            "heartbeats_received": 0
        }
        
        # Start the node
        self.logger.info(f"Starting server for node {node_id}...")
        self._start_server()
        self.logger.info(f"Starting background tasks for node {node_id}...")
        self._start_background_tasks()
        self.logger.info(f"Node {node_id} initialization complete")
    
    def _reset_election_timeout(self) -> float:
        """Reset election timeout with random jitter"""
        timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
        new_deadline = time.time() + timeout
        
        # Create logger if needed
        if not hasattr(self, 'logger'):
            self.logger = logging.getLogger(f"raft.{self.node_id}")
        
        self.logger.debug(f"Reset election timeout: timeout={timeout:.2f}s, new_deadline={new_deadline:.2f}, current={time.time():.2f}")
        return new_deadline
    
    def _start_server(self):
        """Start the network server for this node"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            
            self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
            self.server_thread.start()
            
            self.logger.info(f"RAFT node {self.node_id} listening on {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to start server: {e}")
            raise
    
    def _start_background_tasks(self):
        """Start background tasks for RAFT operations"""
        # Election timeout monitor
        threading.Thread(target=self._election_timeout_monitor, daemon=True).start()
        
        # Heartbeat sender (if leader)
        threading.Thread(target=self._heartbeat_sender, daemon=True).start()
        
        # Log application worker
        threading.Thread(target=self._log_applier, daemon=True).start()
        
        # Health monitor
        threading.Thread(target=self._health_monitor, daemon=True).start()
    
    def _server_loop(self):
        """Main server loop to handle incoming connections"""
        while self.running:
            try:
                client_socket, _ = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True
                ).start()
            except Exception as e:
                if self.running:
                    self.logger.error(f"Server loop error: {e}")
                break
    
    def _handle_client(self, client_socket):
        """Handle a client connection"""
        try:
            # Use proper protocol with 4-byte length prefix
            from shared.utils import send_data, receive_data
            message = receive_data(client_socket)
            
            if message:
                response = self._process_message(message)
                
                if response:
                    send_data(client_socket, response)
        except ConnectionResetError:
            # Client disconnected, this is normal
            pass
        except Exception as e:
            self.logger.debug(f"Error handling client: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass

    
    def _process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process incoming RAFT messages"""
        # Check if node is partitioned (for testing)
        if getattr(self, 'is_partitioned', False):
            return None  # Drop messages during partition
        
        msg_type = message.get("type")
        
        try:
            if msg_type == MessageType.VOTE_REQUEST.value:
                return self._handle_vote_request(VoteRequest(**message["data"]))
            elif msg_type == MessageType.APPEND_ENTRIES.value:
                data = message["data"].copy()
                # Convert entry dictionaries to LogEntry objects with proper error handling
                entries = []
                for entry_data in data.get("entries", []):
                    try:
                        # Ensure all required fields are present
                        if isinstance(entry_data, dict):
                            # Create LogEntry with proper defaults
                            entry = LogEntry(
                                term=entry_data.get("term", 0),
                                index=entry_data.get("index", 0),
                                operation=entry_data.get("operation", {}),
                                committed=entry_data.get("committed", False),
                                timestamp=entry_data.get("timestamp", time.time()),
                                checksum=entry_data.get("checksum", "")
                            )
                            entries.append(entry)
                        else:
                            self.logger.warning(f"Invalid entry data format: {entry_data}")
                    except Exception as entry_error:
                        self.logger.error(f"Error converting entry: {entry_error}")
                        continue
                
                data["entries"] = entries
                return self._handle_append_entries(AppendEntries(**data))
            elif msg_type == MessageType.INSTALL_SNAPSHOT.value:
                return self._handle_install_snapshot(message["data"])
            else:
                self.logger.warning(f"Unknown message type: {msg_type}")
                return {"error": "Unknown message type"}
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            return {"error": str(e)}
    
    def _handle_vote_request(self, request: VoteRequest) -> Dict[str, Any]:
        """Handle vote request from candidate"""
        with self.lock:
            # If request term is older, reject
            if request.term < self.current_term:
                return {
                    "type": MessageType.VOTE_RESPONSE.value,
                    "data": VoteResponse(
                        term=self.current_term,
                        vote_granted=False,
                        voter_id=self.node_id
                    ).__dict__
                }
            
            # If request term is newer, update our term and become follower
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self._become_follower()
            
            # Check if we can vote for this candidate
            vote_granted = False
            if (self.voted_for is None or self.voted_for == request.candidate_id):
                # Check if candidate's log is at least as up-to-date as ours
                last_log_index = len(self.log) - 1 if self.log else -1
                last_log_term = self.log[-1].term if self.log else 0
                
                if (request.last_log_term > last_log_term or
                    (request.last_log_term == last_log_term and
                    request.last_log_index >= last_log_index)):
                    vote_granted = True
                    self.voted_for = request.candidate_id
                    self.last_heartbeat = time.time()  # Reset election timeout
            
            return {
                "type": MessageType.VOTE_RESPONSE.value,
                "data": VoteResponse(
                    term=self.current_term,
                    vote_granted=vote_granted,
                    voter_id=self.node_id
                ).__dict__
            }
    
    def _handle_append_entries(self, request: AppendEntries) -> Dict[str, Any]:
        """Handle append entries (heartbeat/log replication) from leader"""
        with self.lock:
            success = False
            match_index = 0
            conflict_index = 0
            
            # If request term is older, reject
            if request.term < self.current_term:
                return {
                    "type": MessageType.APPEND_RESPONSE.value,
                    "data": AppendResponse(
                        term=self.current_term,
                        success=False,
                        follower_id=self.node_id,
                        match_index=match_index,
                        conflict_index=conflict_index
                    ).__dict__
                }
            
            # If request term is newer, update our term and become follower
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
            
            self._become_follower()
            self.last_heartbeat = time.time()
            self.election_deadline = self._reset_election_timeout()  # Reset election timeout
            self.metrics["heartbeats_received"] += 1
            
            # Check log consistency
            if request.prev_log_index >= 0:
                if (request.prev_log_index >= len(self.log) or
                    self.log[request.prev_log_index].term != request.prev_log_term):
                    # Log inconsistency - find conflict index
                    conflict_index = min(request.prev_log_index, len(self.log) - 1)
                    while conflict_index > 0 and self.log[conflict_index].term == request.prev_log_term:
                        conflict_index -= 1
                    
                    return {
                        "type": MessageType.APPEND_RESPONSE.value,
                        "data": AppendResponse(
                            term=self.current_term,
                            success=False,
                            follower_id=self.node_id,
                            match_index=match_index,
                            conflict_index=conflict_index
                        ).__dict__
                    }
            
            # Append new entries
            if request.entries:
                # Remove conflicting entries
                insert_index = request.prev_log_index + 1
                if insert_index < len(self.log):
                    self.log = self.log[:insert_index]
                
                # Append new entries
                for entry in request.entries:
                    # Verify checksum
                    expected_checksum = entry.checksum
                    entry.checksum = ""  # Reset for verification
                    data = f"{entry.term}{entry.index}{json.dumps(entry.operation, sort_keys=True)}"
                    actual_checksum = hashlib.sha256(data.encode()).hexdigest()[:16]
                    
                    if actual_checksum != expected_checksum:
                        self.logger.error(f"Checksum mismatch for log entry {entry.index}")
                        return {
                            "type": MessageType.APPEND_RESPONSE.value,
                            "data": AppendResponse(
                                term=self.current_term,
                                success=False,
                                follower_id=self.node_id,
                                match_index=match_index,
                                conflict_index=conflict_index
                            ).__dict__
                        }
                    
                    entry.checksum = expected_checksum
                    self.log.append(entry)
                    self.metrics["log_entries_received"] += 1
            
            # Update commit index
            if request.leader_commit > self.commit_index:
                self.commit_index = min(request.leader_commit, len(self.log) - 1)
            
            match_index = len(self.log) - 1
            success = True
            
            return {
                "type": MessageType.APPEND_RESPONSE.value,
                "data": AppendResponse(
                    term=self.current_term,
                    success=success,
                    follower_id=self.node_id,
                    match_index=match_index,
                    conflict_index=conflict_index
                ).__dict__
            }
    def _handle_install_snapshot(self, _: Dict[str, Any]) -> Dict[str, Any]:
        """Handle snapshot installation from leader"""
        # Placeholder for snapshot functionality
        return {
            "type": MessageType.SNAPSHOT_RESPONSE.value,
            "data": {"success": False, "error": "Snapshots not implemented"}
        }
    
    def _become_follower(self):
        """Transition to follower state"""
        if self.state != NodeState.FOLLOWER:
            self.logger.info(f"Node {self.node_id} becoming follower in term {self.current_term}")
            self.state = NodeState.FOLLOWER
            if self.leadership_callback:
                self.leadership_callback(False)
    
    def _become_candidate(self):
        """Transition to candidate state and start election"""
        with self.lock:
            # Only transition if we're not already a leader
            if self.state == NodeState.LEADER:
                self.logger.debug(f"Already leader for term {self.current_term}, not becoming candidate")
                return
                
            prev_state = self.state
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            self.election_deadline = self._reset_election_timeout()
            self.metrics["elections_started"] += 1
            
            self.logger.info(f"Node {self.node_id} becoming candidate for term {self.current_term} (was {prev_state.value})")
            
            # Reset vote counters
            self.votes_received = 0
            self.current_election_term = self.current_term
            
            # Start election in a new thread to avoid blocking
            election_thread = threading.Thread(
                name=f"election-{self.node_id}-term-{self.current_term}",
                target=self._conduct_election, 
                daemon=True
            )
            election_thread.start()
    
    def _become_leader(self):
        """Transition to leader state"""
        with self.lock:
            self.state = NodeState.LEADER
            self.logger.info(f"Node {self.node_id} becoming leader for term {self.current_term}")
            self.metrics["elections_won"] += 1
            
            # Initialize leader state
            voting_nodes = self.cluster.get_voting_nodes()
            last_log_index = len(self.log) - 1 if self.log else -1
            
            for node_id in voting_nodes:
                if node_id != self.node_id:
                    self.next_index[node_id] = last_log_index + 1
                    self.match_index[node_id] = 0
            
            if self.leadership_callback:
                self.leadership_callback(True)
            
            # Send immediate heartbeats
            self._send_heartbeats()
    
    def _conduct_election(self):
        """Conduct leader election"""
        with self.lock:
            voting_nodes = self.cluster.get_voting_nodes()
            total_votes = len(voting_nodes)
            votes_needed = total_votes // 2 + 1
            self.votes_received = 1  # Vote for self
            self.current_election_term = self.current_term
            
            self.logger.info(f"Election: total_votes={total_votes}, votes_needed={votes_needed}, votes_received={self.votes_received}")
            
            last_log_index = len(self.log) - 1 if self.log else -1
            last_log_term = self.log[-1].term if self.log else 0
            
            vote_request = VoteRequest(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term
            )
        
        # Send vote requests to all other nodes
        for node_id, node_info in voting_nodes.items():
            if node_id != self.node_id:
                threading.Thread(
                    target=self._send_vote_request,
                    args=(node_id, node_info, vote_request),
                    daemon=True
                ).start()
        
        # Wait for votes
        start_time = time.time()
        while (time.time() - start_time < self.election_timeout_min and
               self.state == NodeState.CANDIDATE and
               self.votes_received < votes_needed and
               self.current_term == self.current_election_term):
            time.sleep(0.1)
        
        # Check if we won the election
        with self.lock:
            self.logger.info(f"Election result: state={self.state.value}, votes_received={self.votes_received}, votes_needed={votes_needed}, term_match={self.current_term == self.current_election_term}")
            if (self.state == NodeState.CANDIDATE and 
                self.votes_received >= votes_needed and
                self.current_term == self.current_election_term):
                self._become_leader()
    
    def _send_vote_request(self, node_id: str, node_info: Dict[str, Any], 
                          vote_request: VoteRequest):
        """Send vote request to a specific node"""
        # Check if this node is partitioned
        if getattr(self, 'is_partitioned', False):
            return  # Don't send messages during partition
        
        try:
            self.logger.debug(f"Sending vote request to {node_id} at {node_info['host']}:{node_info['port']} for term {vote_request.term}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            
            try:
                sock.connect((node_info["host"], node_info["port"]))
            except (ConnectionRefusedError, socket.timeout, OSError) as e:
                self.logger.debug(f"Connection failed to {node_id} at {node_info['host']}:{node_info['port']}: {e}")
                return
            
            message = {
                "type": MessageType.VOTE_REQUEST.value,
                "data": vote_request.__dict__
            }
            
            self.logger.debug(f"Vote request message to {node_id}: {message}")
            
            # Use proper protocol with 4-byte length prefix
            from shared.utils import send_data, receive_data
            send_data(sock, message)
            
            try:
                response = receive_data(sock)
                
                if response:
                    self.logger.debug(f"Received vote response data from {node_id}: {response}")
                    if response.get("type") == MessageType.VOTE_RESPONSE.value:
                        vote_response = VoteResponse(**response["data"])
                        
                        self.logger.debug(f"Processed vote from {node_id}: granted={vote_response.vote_granted}, term={vote_response.term}")
                        
                        with self.lock:
                            # If we received a higher term, step down
                            if vote_response.term > self.current_term:
                                self.logger.info(f"Received higher term {vote_response.term} > {self.current_term} from {node_id}, stepping down")
                                self.current_term = vote_response.term
                                self.voted_for = None
                                self._become_follower()
                            elif (vote_response.term == self.current_term and
                                  vote_response.vote_granted and
                                  self.state == NodeState.CANDIDATE and
                                  self.current_term == self.current_election_term):
                                # Count the vote
                                self.votes_received += 1
                                self.logger.info(f"Received vote from {node_id}, total votes: {self.votes_received}")
                else:
                    self.logger.debug(f"Received empty response from {node_id}")
            except socket.timeout:
                self.logger.debug(f"Timeout waiting for vote response from {node_id}")
        
        except Exception as e:
            self.logger.debug(f"Failed to get vote from {node_id}: {e}")
        finally:
            try:
                sock.close()
            except:
                pass
    
    def _election_timeout_monitor(self):
        """Monitor election timeout and trigger elections"""
        while self.running:
            try:
                current_time = time.time()
                time_until_deadline = self.election_deadline - current_time
                
                if self.state == NodeState.LEADER:
                    # Leaders don't have election timeouts, but reset for when they step down
                    self.election_deadline = self._reset_election_timeout()
                elif self.state in [NodeState.FOLLOWER, NodeState.CANDIDATE]:
                    # More verbose logging near deadline
                    if time_until_deadline < 2.0:
                        self.logger.debug(f"Election deadline approaching: state={self.state.value}, " 
                                         f"time_left={time_until_deadline:.2f}s")
                    
                    if current_time > self.election_deadline:
                        self.logger.info(f"Election timeout triggered: state={self.state.value}, "
                                        f"timeout_diff={current_time-self.election_deadline:.2f}s")
                        
                        # Add small random delay to prevent synchronized elections
                        jitter_delay = random.uniform(0.01, 0.2)  # Increased max jitter
                        time.sleep(jitter_delay)
                        
                        # Critical section - use lock to avoid race conditions
                        with self.lock:
                            # Double-check that timeout is still valid after acquiring the lock
                            if (self.state in [NodeState.FOLLOWER, NodeState.CANDIDATE] and 
                                time.time() > self.election_deadline):
                                self._become_candidate()
                
                # Adaptive sleep time based on proximity to deadline
                if time_until_deadline > 1.0:
                    sleep_time = 0.1
                elif time_until_deadline > 0:
                    sleep_time = min(0.05, time_until_deadline / 4)
                else:
                    sleep_time = 0.05
                
                time.sleep(sleep_time)
                
            except Exception as e:
                self.logger.error(f"Election timeout monitor error: {e}")
                time.sleep(0.1)  # Prevent tight loop on errors
    
    def _heartbeat_sender(self):
        """Send heartbeats as leader"""
        while self.running:
            try:
                if self.state == NodeState.LEADER:
                    self._send_heartbeats()
                    self.metrics["heartbeats_sent"] += 1
                
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.error(f"Heartbeat sender error: {e}")
    
    def _send_heartbeats(self):
        """Send heartbeat/append entries to all followers"""
        voting_nodes = self.cluster.get_voting_nodes()
        
        for node_id, node_info in voting_nodes.items():
            if node_id != self.node_id:
                threading.Thread(
                    target=self._send_append_entries,
                    args=(node_id, node_info),
                    daemon=True
                ).start()
    
    def _send_append_entries(self, node_id: str, node_info: Dict[str, Any]):
        """Send append entries to a specific follower"""
        # Check if this node is partitioned
        if getattr(self, 'is_partitioned', False):
            return  # Don't send messages during partition
        
        try:
            with self.lock:
                next_index = self.next_index.get(node_id, 0)
                prev_log_index = next_index - 1
                prev_log_term = 0
                
                if prev_log_index >= 0 and prev_log_index < len(self.log):
                    prev_log_term = self.log[prev_log_index].term
                
                # Determine entries to send
                entries = []
                if next_index < len(self.log):
                    entries = self.log[next_index:next_index + 5]  # Reduced batch size
                
                append_request = AppendEntries(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index
                )
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)  # Reduced timeout
            
            try:
                sock.connect((node_info["host"], node_info["port"]))
            except (ConnectionRefusedError, socket.timeout, OSError):
                # Mark node as potentially offline and return
                with self.lock:
                    if node_id in self.cluster.nodes:
                        self.cluster.nodes[node_id]["status"] = "offline"
                return
            
            message = {
                "type": MessageType.APPEND_ENTRIES.value,
                "data": {
                    "term": append_request.term,
                    "leader_id": append_request.leader_id,
                    "prev_log_index": append_request.prev_log_index,
                    "prev_log_term": append_request.prev_log_term,
                    "entries": [entry.__dict__ for entry in append_request.entries],
                    "leader_commit": append_request.leader_commit
                }
            }
            
            # Use proper protocol with 4-byte length prefix
            from shared.utils import send_data, receive_data
            send_data(sock, message)
            response = receive_data(sock)
            
            if response:
                if response.get("type") == MessageType.APPEND_RESPONSE.value:
                    append_response = AppendResponse(**response["data"])
                    self._handle_append_response(node_id, append_response)
                    
                    # Mark node as online
                    with self.lock:
                        if node_id in self.cluster.nodes:
                            self.cluster.nodes[node_id]["status"] = "online"
                            self.cluster.nodes[node_id]["last_seen"] = time.time()
        
        except Exception as e:
            self.logger.debug(f"Failed to send append entries to {node_id}: {e}")
            # Mark node as potentially offline
            with self.lock:
                if node_id in self.cluster.nodes:
                    self.cluster.nodes[node_id]["status"] = "offline"
        finally:
            try:
                sock.close()
            except:
                pass

    
    def _handle_append_response(self, node_id: str, response: AppendResponse):
        """Handle append entries response from follower"""
        with self.lock:
            # If response term is higher, step down
            if response.term > self.current_term:
                self.current_term = response.term
                self.voted_for = None
                self._become_follower()
                return
            
            # Only process if we're still leader and term matches
            if self.state != NodeState.LEADER or response.term != self.current_term:
                return
            
            if response.success:
                # Update next_index and match_index
                self.next_index[node_id] = response.match_index + 1
                self.match_index[node_id] = response.match_index
                
                # Check if we can advance commit_index
                self._update_commit_index()
            else:
                # Decrement next_index and retry
                if response.conflict_index > 0:
                    self.next_index[node_id] = response.conflict_index
                else:
                    self.next_index[node_id] = max(0, self.next_index[node_id] - 1)
    
    def _update_commit_index(self):
        """Update commit index based on majority agreement"""
        if self.state != NodeState.LEADER:
            return
        
        voting_nodes = self.cluster.get_voting_nodes()
        majority = len(voting_nodes) // 2 + 1
        
        # Find the highest index that's replicated on majority
        for i in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[i].term == self.current_term:
                # Count nodes that have this entry
                replicated_count = 1  # Leader has it
                for node_id in voting_nodes:
                    if (node_id != self.node_id and
                        self.match_index.get(node_id, 0) >= i):
                        replicated_count += 1
                
                if replicated_count >= majority:
                    self.commit_index = i
                    break
    
    def _log_applier(self):
        """Apply committed log entries to state machine"""
        while self.running:
            try:
                if self.last_applied < self.commit_index:
                    with self.lock:
                        for i in range(self.last_applied + 1, self.commit_index + 1):
                            if i < len(self.log):
                                entry = self.log[i]
                                if self.apply_callback:
                                    try:
                                        self.apply_callback(entry)
                                        self.metrics["log_entries_applied"] += 1
                                    except Exception as e:
                                        self.logger.error(f"Error applying log entry {i}: {e}")
                                
                                self.last_applied = i
                
                time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Log applier error: {e}")
    
    def _health_monitor(self):
        """Monitor cluster health and update node statuses"""
        while self.running:
            try:
                current_time = time.time()
                
                with self.lock:
                    for node_id, node_info in self.cluster.nodes.items():
                        if node_id != self.node_id:
                            # Check if node has been seen recently
                            last_seen = node_info.get("last_seen", 0)
                            if current_time - last_seen > 30:  # 30 second timeout
                                if node_info["status"] != "offline":
                                    node_info["status"] = "offline"
                                    self.logger.warning(f"Node {node_id} marked as offline")
                
                time.sleep(10)  # Check every 10 seconds
            except Exception as e:
                self.logger.error(f"Health monitor error: {e}")
    
    def add_log_entry(self, operation: Dict[str, Any]) -> bool:
        """Add a new log entry (only for leader)"""
        with self.lock:
            if self.state != NodeState.LEADER:
                return False
            
            entry = LogEntry(
                term=self.current_term,
                index=len(self.log),
                operation=operation
            )
            
            self.log.append(entry)
            
            # Trigger immediate replication
            threading.Thread(target=self._send_heartbeats, daemon=True).start()
            
            return True
    
    def set_apply_callback(self, callback: Callable[[LogEntry], Any]):
        """Set callback for applying log entries to state machine"""
        self.apply_callback = callback
    
    def set_leadership_callback(self, callback: Callable[[bool], None]):
        """Set callback for leadership changes"""
        self.leadership_callback = callback
    
    def get_status(self) -> Dict[str, Any]:
        """Get current node status"""
        with self.lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "voted_for": self.voted_for,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "cluster_size": len(self.cluster.get_voting_nodes()),
                "is_leader": self.state == NodeState.LEADER,
                "metrics": self.metrics.copy()
            }
    
    def shutdown(self):
        """Shutdown the RAFT node"""
        self.logger.info(f"Shutting down RAFT node {self.node_id}")
        self.running = False
        
        # Close server socket with proper error handling
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # Socket may already be closed
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None
        
        # Stop server thread
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
            if self.server_thread.is_alive():
                self.logger.warning(f"Server thread for {self.node_id} did not stop gracefully")
        
        # Allow time for cleanup
        time.sleep(0.1)


class RaftCluster:
    """Manages a RAFT cluster for database replication"""
    
    def __init__(self, cluster_config: Dict[str, Any]):
        """Initialize RAFT cluster.
        
        Args:
            cluster_config: Configuration with node information
        """
        self.config = cluster_config
        self.nodes: Dict[str, RaftNode] = {}
        self.local_node: Optional[RaftNode] = None
        self.logger = logging.getLogger("raft.cluster")
        
        # Network partition simulation (for testing)
        self.is_partitioned = False
    
    def add_node(self, node_id: str, host: str, port: int, 
                 is_local: bool = False) -> RaftNode:
        """Add a node to the cluster"""
        # Get all other nodes for peer list
        peers = [(nid, info["host"], info["port"]) 
                for nid, info in self.config.get("nodes", {}).items()
                if nid != node_id]
        
        node = RaftNode(
            node_id=node_id,
            host=host,
            port=port,
            peers=peers,
            election_timeout_min=self.config.get("election_timeout_min", 5.0),
            election_timeout_max=self.config.get("election_timeout_max", 10.0),
            heartbeat_interval=self.config.get("heartbeat_interval", 2.0)
        )
        
        # Set up callbacks
        node.set_apply_callback(self._apply_to_database)
        node.set_leadership_callback(lambda is_leader: self._on_leadership_change(node_id, is_leader))
        
        self.nodes[node_id] = node
        
        if is_local:
            self.local_node = node
        
        return node
    
    def _apply_to_database(self, entry: LogEntry):
        """Apply log entry to database"""
        if self.database_callback:
            try:
                self.database_callback(entry.operation)
            except Exception as e:
                self.logger.error(f"Error applying operation to database: {e}")
    
    def _on_leadership_change(self, node_id: str, is_leader: bool):
        """Handle leadership changes"""
        if is_leader:
            self.logger.info(f"Node {node_id} became leader")
        else:
            self.logger.info(f"Node {node_id} lost leadership")
        
        if self.leadership_change_callback:
            self.leadership_change_callback(node_id, is_leader)
    
    def set_database_callback(self, callback: Callable[[Dict[str, Any]], Any]):
        """Set callback for applying operations to database"""
        self.database_callback = callback
    
    def set_leadership_change_callback(self, callback: Callable[[str, bool], None]):
        """Set callback for leadership changes"""
        self.leadership_change_callback = callback
    
    def submit_operation(self, operation: Dict[str, Any]) -> bool:
        """Submit an operation to the cluster"""
        # Check if cluster is partitioned
        if getattr(self, 'is_partitioned', False):
            return False  # Don't accept operations during partition
        
        if self.local_node and self.local_node.state == NodeState.LEADER:
            return self.local_node.add_log_entry(operation)
        
        # Find current leader and forward request
        for node in self.nodes.values():
            if node.state == NodeState.LEADER:
                return node.add_log_entry(operation)
        
        return False
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get status of entire cluster"""
        status = {
            "nodes": {},
            "leader": None,
            "term": 0
        }
        
        for node_id, node in self.nodes.items():
            node_status = node.get_status()
            status["nodes"][node_id] = node_status
            
            if node_status["is_leader"]:
                status["leader"] = node_id
                status["term"] = node_status["term"]
        
        return status
    
    def shutdown(self):
        """Shutdown the entire cluster"""
        for node in self.nodes.values():
            node.shutdown()
