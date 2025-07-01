"""
Raft Consensus Algorithm Implementation
Supporting leader election, log replication, and safety properties
"""

import asyncio
import time
import json
import random
import logging
from enum import Enum
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
import threading
from concurrent.futures import Future


logger = logging.getLogger(__name__)


class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """Raft log entry"""

    term: int
    index: int
    command: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        return cls(
            term=data["term"],
            index=data["index"],
            command=data["command"],
            timestamp=data.get("timestamp", time.time()),
        )


@dataclass
class VoteRequest:
    """RequestVote RPC request"""

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteResponse:
    """RequestVote RPC response"""

    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    """AppendEntries RPC request"""

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """AppendEntries RPC response"""

    term: int
    success: bool
    last_log_index: int = 0


class RaftConfig:
    """Raft configuration"""

    def __init__(self):
        self.election_timeout_min = 0.15  # 150ms
        self.election_timeout_max = 0.3  # 300ms
        self.heartbeat_interval = 0.05  # 50ms
        self.log_compaction_threshold = 1000
        self.max_log_entries_per_request = 100
        self.snapshot_interval = 10000


class RaftNode:
    """Raft consensus node implementation"""

    def __init__(
        self, node_id: str, peers: List[str], config: Optional[RaftConfig] = None
    ):
        self.node_id = node_id
        self.peers = set(peers)
        self.config = config or RaftConfig()

        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []

        # Volatile state
        self.commit_index = 0
        self.last_applied = 0
        self.state = RaftState.FOLLOWER

        # Leader state
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Timing
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()

        # Threading
        self._lock = threading.RLock()
        self._running = False
        self._background_task: Optional[asyncio.Task] = None

        # Callbacks
        self.on_command_applied: Optional[Callable[[Dict[str, Any]], None]] = None
        self.on_state_change: Optional[Callable[[RaftState], None]] = None
        self.on_leader_change: Optional[Callable[[Optional[str]], None]] = None

        # Communication interfaces (to be set by parent)
        self.send_vote_request: Optional[
            Callable[[str, VoteRequest], Future[VoteResponse]]
        ] = None
        self.send_append_entries: Optional[
            Callable[[str, AppendEntriesRequest], Future[AppendEntriesResponse]]
        ] = None

        # Current leader
        self.current_leader: Optional[str] = None

        # Snapshot state
        self.last_included_index = 0
        self.last_included_term = 0

    def _random_election_timeout(self) -> float:
        """Generate random election timeout"""
        return random.uniform(
            self.config.election_timeout_min, self.config.election_timeout_max
        )

    def start(self):
        """Start the Raft node"""
        with self._lock:
            if self._running:
                return
            self._running = True

        # Start background tasks
        asyncio.create_task(self._background_loop())
        logger.info(f"Raft node {self.node_id} started")

    def stop(self):
        """Stop the Raft node"""
        with self._lock:
            self._running = False

        if self._background_task:
            self._background_task.cancel()

        logger.info(f"Raft node {self.node_id} stopped")

    async def _background_loop(self):
        """Main background loop for Raft operations"""
        while self._running:
            try:
                if self.state == RaftState.LEADER:
                    await self._leader_loop()
                else:
                    await self._follower_candidate_loop()

                await asyncio.sleep(0.01)  # 10ms

            except Exception as e:
                logger.error(f"Error in Raft background loop: {e}")
                await asyncio.sleep(0.1)

    async def _leader_loop(self):
        """Leader-specific operations"""
        now = time.time()

        # Send heartbeats
        if now - self.last_heartbeat >= self.config.heartbeat_interval:
            await self._send_heartbeats()
            self.last_heartbeat = now

    async def _follower_candidate_loop(self):
        """Follower/Candidate operations"""
        now = time.time()

        # Check for election timeout
        if now - self.last_heartbeat >= self.election_timeout:
            await self._start_election()

    async def _send_heartbeats(self):
        """Send heartbeat messages to all peers"""
        if not self.send_append_entries:
            return

        tasks = []
        for peer in self.peers:
            task = asyncio.create_task(self._send_append_entries_to_peer(peer))
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_append_entries_to_peer(self, peer: str):
        """Send AppendEntries to a specific peer"""
        try:
            with self._lock:
                next_index = self.next_index.get(peer, len(self.log))
                prev_log_index = next_index - 1
                prev_log_term = 0

                if prev_log_index > 0 and prev_log_index <= len(self.log):
                    prev_log_term = self.log[prev_log_index - 1].term

                # Get entries to send
                entries = []
                if next_index <= len(self.log):
                    end_index = min(
                        next_index + self.config.max_log_entries_per_request,
                        len(self.log) + 1,
                    )
                    entries = self.log[next_index - 1 : end_index - 1]

                request = AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=entries,
                    leader_commit=self.commit_index,
                )

            if self.send_append_entries:
                response = await self.send_append_entries(peer, request)
                await self._handle_append_entries_response(peer, request, response)

        except Exception as e:
            logger.error(f"Error sending AppendEntries to {peer}: {e}")

    async def _handle_append_entries_response(
        self, peer: str, request: AppendEntriesRequest, response: AppendEntriesResponse
    ):
        """Handle AppendEntries response"""
        with self._lock:
            # Step down if we see a higher term
            if response.term > self.current_term:
                self._step_down(response.term)
                return

            if response.success:
                # Update next_index and match_index
                self.next_index[peer] = (
                    request.prev_log_index + len(request.entries) + 1
                )
                self.match_index[peer] = request.prev_log_index + len(request.entries)

                # Update commit index
                self._update_commit_index()
            else:
                # Decrement next_index and retry
                self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)

    def _update_commit_index(self):
        """Update commit index based on majority agreement"""
        if self.state != RaftState.LEADER:
            return

        # Find the highest index that's replicated on majority
        for index in range(self.commit_index + 1, len(self.log) + 1):
            count = 1  # Count self
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= index:
                    count += 1

            # Check if majority agrees and entry is from current term
            majority_needed = (len(self.peers) + 1) // 2 + 1  # +1 to include self in total count
            if count >= majority_needed:
                if (
                    index <= len(self.log)
                    and self.log[index - 1].term == self.current_term
                ):
                    self.commit_index = index

    async def _start_election(self):
        """Start leader election"""
        with self._lock:
            self.current_term += 1
            self.state = RaftState.CANDIDATE
            self.voted_for = self.node_id
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()

            if self.on_state_change:
                self.on_state_change(self.state)

        logger.info(
            f"Node {self.node_id} starting election for term {self.current_term}"
        )

        # Send vote requests
        votes = 1  # Vote for self
        vote_tasks = []

        if not self.send_vote_request:
            return

        for peer in self.peers:
            task = asyncio.create_task(self._request_vote_from_peer(peer))
            vote_tasks.append(task)

        if vote_tasks:
            responses = await asyncio.gather(*vote_tasks, return_exceptions=True)

            for response in responses:
                if isinstance(response, VoteResponse) and response.vote_granted:
                    votes += 1
                elif (
                    isinstance(response, VoteResponse)
                    and response.term > self.current_term
                ):
                    self._step_down(response.term)
                    return

        # Check if won election
        with self._lock:
            if self.state == RaftState.CANDIDATE and votes > len(self.peers) // 2 + 1:
                self._become_leader()

    async def _request_vote_from_peer(self, peer: str) -> Optional[VoteResponse]:
        """Request vote from a peer"""
        try:
            with self._lock:
                last_log_index = len(self.log)
                last_log_term = 0
                if self.log:
                    last_log_term = self.log[-1].term

                request = VoteRequest(
                    term=self.current_term,
                    candidate_id=self.node_id,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term,
                )

            if self.send_vote_request:
                return await self.send_vote_request(peer, request)

        except Exception as e:
            logger.error(f"Error requesting vote from {peer}: {e}")

        return None

    def _become_leader(self):
        """Transition to leader state"""
        with self._lock:
            self.state = RaftState.LEADER
            self.current_leader = self.node_id

            # Initialize leader state
            for peer in self.peers:
                self.next_index[peer] = len(self.log) + 1
                self.match_index[peer] = 0

            self.last_heartbeat = time.time()

            if self.on_state_change:
                self.on_state_change(self.state)
            if self.on_leader_change:
                self.on_leader_change(self.node_id)

        logger.info(f"Node {self.node_id} became leader for term {self.current_term}")

    def _step_down(self, new_term: int):
        """Step down to follower state"""
        with self._lock:
            old_state = self.state
            self.current_term = new_term
            self.voted_for = None
            self.state = RaftState.FOLLOWER
            self.last_heartbeat = time.time()
            self.election_timeout = self._random_election_timeout()

            if old_state == RaftState.LEADER:
                self.current_leader = None
                if self.on_leader_change:
                    self.on_leader_change(None)

            if self.on_state_change:
                self.on_state_change(self.state)

        logger.info(f"Node {self.node_id} stepped down to follower for term {new_term}")

    # RPC Handlers

    def handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        """Handle RequestVote RPC"""
        with self._lock:
            # Step down if we see a higher term
            if request.term > self.current_term:
                self._step_down(request.term)

            vote_granted = False

            # Grant vote if:
            # 1. Term is at least as large as ours
            # 2. Haven't voted for anyone else in this term
            # 3. Candidate's log is at least as up-to-date as ours
            if (
                request.term >= self.current_term
                and (self.voted_for is None or self.voted_for == request.candidate_id)
                and self._is_log_up_to_date(
                    request.last_log_index, request.last_log_term
                )
            ):

                vote_granted = True
                self.voted_for = request.candidate_id
                self.last_heartbeat = time.time()  # Reset election timeout

            return VoteResponse(term=self.current_term, vote_granted=vote_granted)

    def handle_append_entries(
        self, request: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        """Handle AppendEntries RPC"""
        with self._lock:
            # Step down if we see a higher term
            if request.term > self.current_term:
                self._step_down(request.term)

            success = False

            if request.term >= self.current_term:
                # Valid leader
                self.current_leader = request.leader_id
                self.last_heartbeat = time.time()

                if self.state != RaftState.FOLLOWER:
                    self.state = RaftState.FOLLOWER
                    if self.on_state_change:
                        self.on_state_change(self.state)

                # Check log consistency
                if request.prev_log_index == 0 or (
                    request.prev_log_index <= len(self.log)
                    and self.log[request.prev_log_index - 1].term
                    == request.prev_log_term
                ):

                    success = True

                    # Append new entries
                    if request.entries:
                        # Remove conflicting entries
                        self.log = self.log[: request.prev_log_index]
                        self.log.extend(request.entries)

                    # Update commit index
                    if request.leader_commit > self.commit_index:
                        self.commit_index = min(request.leader_commit, len(self.log))
                        self._apply_committed_entries()

            return AppendEntriesResponse(
                term=self.current_term, success=success, last_log_index=len(self.log)
            )

    def _is_log_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """Check if candidate's log is at least as up-to-date as ours"""
        if not self.log:
            return True

        our_last_term = self.log[-1].term
        our_last_index = len(self.log)

        if last_log_term > our_last_term:
            return True
        elif last_log_term == our_last_term:
            return last_log_index >= our_last_index
        else:
            return False

    def _apply_committed_entries(self):
        """Apply committed log entries"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]

            if self.on_command_applied:
                self.on_command_applied(entry.command)

    # Client interface

    async def append_command(self, command: Dict[str, Any]) -> bool:
        """Append a command to the log (leader only)"""
        if self.state != RaftState.LEADER:
            return False

        with self._lock:
            entry = LogEntry(
                term=self.current_term, index=len(self.log) + 1, command=command
            )
            self.log.append(entry)

        # Wait for replication (simplified)
        await asyncio.sleep(0.1)

        with self._lock:
            # For single-node clusters, update commit index immediately
            if len(self.peers) == 0:
                self._update_commit_index()
                self._apply_committed_entries()  # Apply the committed entries
            return self.commit_index >= entry.index

    # Aliases for backward compatibility
    async def add_command(self, command: Dict[str, Any]) -> bool:
        """Alias for append_command for backward compatibility"""
        return await self.append_command(command)

    def is_leader(self) -> bool:
        """Check if this node is the leader"""
        with self._lock:
            return self.state == RaftState.LEADER

    def get_leader(self) -> Optional[str]:
        """Get current leader"""
        with self._lock:
            return self.current_leader

    def get_state(self) -> RaftState:
        """Get current state"""
        with self._lock:
            return self.state

    def get_log_info(self) -> Tuple[int, int, int]:
        """Get log information: (length, commit_index, last_applied)"""
        with self._lock:
            return len(self.log), self.commit_index, self.last_applied

    # Persistence interface

    def get_persistent_state(self) -> Dict[str, Any]:
        """Get state that needs to be persisted"""
        with self._lock:
            return {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "log": [entry.to_dict() for entry in self.log],
                "last_included_index": self.last_included_index,
                "last_included_term": self.last_included_term,
            }

    def restore_persistent_state(self, state: Dict[str, Any]):
        """Restore persisted state"""
        with self._lock:
            self.current_term = state.get("current_term", 0)
            self.voted_for = state.get("voted_for")
            self.log = [
                LogEntry.from_dict(entry_data) for entry_data in state.get("log", [])
            ]
            self.last_included_index = state.get("last_included_index", 0)
            self.last_included_term = state.get("last_included_term", 0)

            # Apply any committed entries
            self._apply_committed_entries()
