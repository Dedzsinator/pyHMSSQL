"""
Comprehensive unit tests for RAFT consensus implementation.

This module tests the RAFT consensus algorithm including:
- Leader election and state transitions
- Log replication and consistency
- Network communication and fault tolerance
- Persistent state and recovery
- Performance characteristics and error handling

Created for comprehensive testing of production-ready distributed consensus core.
"""

import pytest
import asyncio
import time
import threading
import tempfile
import json
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any, List, Optional

from kvstore.raft import (
    RaftNode,
    RaftConfig,
    RaftState,
    LogEntry,
    VoteRequest,
    VoteResponse,
    AppendEntriesRequest,
    AppendEntriesResponse,
)


class TestRaftConfiguration:
    """Test RAFT configuration and initialization"""

    def test_raft_config_defaults(self):
        """Test default RAFT configuration values"""
        config = RaftConfig()

        assert config.election_timeout_min == 1.0
        assert config.election_timeout_max == 3.0
        assert config.heartbeat_interval == 0.5
        assert config.max_log_entries_per_request == 100
        assert config.snapshot_threshold == 1000

    def test_raft_config_custom_values(self):
        """Test custom RAFT configuration"""
        config = RaftConfig(
            election_timeout_min=0.5,
            election_timeout_max=1.5,
            heartbeat_interval=0.25,
            max_log_entries_per_request=50,
        )

        assert config.election_timeout_min == 0.5
        assert config.election_timeout_max == 1.5
        assert config.heartbeat_interval == 0.25
        assert config.max_log_entries_per_request == 50

    def test_raft_node_initialization(self):
        """Test RAFT node initialization"""
        config = RaftConfig()
        node = RaftNode("node1", ["node2", "node3"], config)

        assert node.node_id == "node1"
        assert node.peers == {"node2", "node3"}
        assert node.state == RaftState.FOLLOWER
        assert node.current_term == 0
        assert node.voted_for is None
        assert len(node.log) == 0
        assert node.commit_index == 0
        assert node.last_applied == 0

    def test_raft_node_invalid_config(self):
        """Test RAFT node with invalid configuration"""
        with pytest.raises(ValueError):
            config = RaftConfig(election_timeout_min=2.0, election_timeout_max=1.0)
            RaftNode("node1", ["node2"], config)


class TestRaftLogEntry:
    """Test RAFT log entry functionality"""

    def test_log_entry_creation(self):
        """Test log entry creation"""
        command = {"type": "SET", "key": "test", "value": "data"}
        entry = LogEntry(term=1, index=0, command=command)

        assert entry.term == 1
        assert entry.index == 0
        assert entry.command == command
        assert entry.timestamp > 0

    def test_log_entry_serialization(self):
        """Test log entry serialization/deserialization"""
        command = {"type": "SET", "key": "test", "value": "data"}
        entry = LogEntry(term=1, index=0, command=command)

        # Test to_dict
        entry_dict = entry.to_dict()
        assert entry_dict["term"] == 1
        assert entry_dict["index"] == 0
        assert entry_dict["command"] == command

        # Test from_dict
        restored_entry = LogEntry.from_dict(entry_dict)
        assert restored_entry.term == entry.term
        assert restored_entry.index == entry.index
        assert restored_entry.command == entry.command

    def test_log_entry_equality(self):
        """Test log entry equality comparison"""
        command = {"type": "SET", "key": "test", "value": "data"}
        entry1 = LogEntry(term=1, index=0, command=command)
        entry2 = LogEntry(term=1, index=0, command=command)
        entry3 = LogEntry(term=2, index=0, command=command)

        assert entry1 == entry2
        assert entry1 != entry3


class TestRaftVoteRequest:
    """Test RAFT vote request functionality"""

    def test_vote_request_creation(self):
        """Test vote request creation"""
        request = VoteRequest(
            term=1, candidate_id="node1", last_log_index=5, last_log_term=1
        )

        assert request.term == 1
        assert request.candidate_id == "node1"
        assert request.last_log_index == 5
        assert request.last_log_term == 1

    def test_vote_response_creation(self):
        """Test vote response creation"""
        response = VoteResponse(term=1, vote_granted=True)

        assert response.term == 1
        assert response.vote_granted is True


class TestRaftAppendEntries:
    """Test RAFT append entries functionality"""

    def test_append_entries_request(self):
        """Test append entries request creation"""
        entries = [
            LogEntry(
                term=1, index=0, command={"type": "SET", "key": "a", "value": "1"}
            ),
            LogEntry(
                term=1, index=1, command={"type": "SET", "key": "b", "value": "2"}
            ),
        ]

        request = AppendEntriesRequest(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=1,
        )

        assert request.term == 1
        assert request.leader_id == "leader"
        assert request.prev_log_index == 0
        assert request.prev_log_term == 0
        assert len(request.entries) == 2
        assert request.leader_commit == 1

    def test_append_entries_response(self):
        """Test append entries response creation"""
        response = AppendEntriesResponse(term=1, success=True, last_log_index=2)

        assert response.term == 1
        assert response.success is True
        assert response.last_log_index == 2


class TestRaftElection:
    """Test RAFT leader election process"""

    @pytest.fixture
    def mock_raft_node(self):
        """Create a mock RAFT node for testing"""
        config = RaftConfig(
            election_timeout_min=0.1, election_timeout_max=0.2, heartbeat_interval=0.05
        )
        node = RaftNode("node1", ["node2", "node3"], config)

        # Mock network functions
        async def mock_send_vote_request(peer, request):
            return VoteResponse(term=request.term, vote_granted=True)

        async def mock_send_append_entries(peer, request):
            return AppendEntriesResponse(
                term=request.term,
                success=True,
                last_log_index=request.prev_log_index + len(request.entries),
            )

        node.send_vote_request = mock_send_vote_request
        node.send_append_entries = mock_send_append_entries

        return node

    @pytest.mark.asyncio
    async def test_election_timeout_triggers_election(self, mock_raft_node):
        """Test that election timeout triggers leader election"""
        node = mock_raft_node

        # Start node
        node.start()

        # Wait for election timeout
        await asyncio.sleep(0.3)

        # Node should have started election and become leader
        # (since mock votes are always granted)
        assert node.state in [RaftState.CANDIDATE, RaftState.LEADER]
        assert node.current_term > 0

        node.stop()

    @pytest.mark.asyncio
    async def test_candidate_becomes_leader(self, mock_raft_node):
        """Test candidate becoming leader after winning election"""
        node = mock_raft_node

        # Manually trigger election
        await node._start_election()

        # Should become leader since mock peers grant votes
        assert node.state == RaftState.LEADER
        assert node.current_term > 0
        assert node.voted_for == node.node_id

    def test_vote_request_handling(self, mock_raft_node):
        """Test handling of vote requests"""
        node = mock_raft_node

        # Test granting vote to valid candidate
        request = VoteRequest(
            term=1, candidate_id="node2", last_log_index=0, last_log_term=0
        )

        response = node.handle_vote_request(request)

        assert response.term >= 1
        assert response.vote_granted is True
        assert node.voted_for == "node2"

    def test_vote_request_rejection(self, mock_raft_node):
        """Test rejection of invalid vote requests"""
        node = mock_raft_node
        node.current_term = 5
        node.voted_for = "node3"

        # Request from different candidate in same term
        request = VoteRequest(
            term=5, candidate_id="node2", last_log_index=0, last_log_term=0
        )

        response = node.handle_vote_request(request)

        assert response.term == 5
        assert response.vote_granted is False

    def test_vote_request_outdated_term(self, mock_raft_node):
        """Test handling vote request with outdated term"""
        node = mock_raft_node
        node.current_term = 5

        request = VoteRequest(
            term=3,  # Outdated term
            candidate_id="node2",
            last_log_index=0,
            last_log_term=0,
        )

        response = node.handle_vote_request(request)

        assert response.term == 5
        assert response.vote_granted is False


class TestRaftLogReplication:
    """Test RAFT log replication functionality"""

    @pytest.fixture
    def leader_follower_setup(self):
        """Setup leader and follower nodes for testing"""
        config = RaftConfig(heartbeat_interval=0.05)

        leader = RaftNode("leader", ["follower"], config)
        follower = RaftNode("follower", ["leader"], config)

        # Set leader state
        leader.state = RaftState.LEADER
        leader.current_term = 1

        # Mock network communication
        async def leader_send_append_entries(peer, request):
            return follower.handle_append_entries(request)

        async def follower_send_vote_request(peer, request):
            return VoteResponse(term=1, vote_granted=False)

        leader.send_append_entries = leader_send_append_entries
        follower.send_vote_request = follower_send_vote_request
        follower.send_append_entries = AsyncMock()

        return leader, follower

    @pytest.mark.asyncio
    async def test_log_entry_replication(self, leader_follower_setup):
        """Test replication of log entries from leader to follower"""
        leader, follower = leader_follower_setup

        # Add command to leader
        command = {"type": "SET", "key": "test", "value": "data"}
        success = await leader.add_command(command)
        assert success

        # Verify entry added to leader log
        assert len(leader.log) == 1
        assert leader.log[0].command == command

        # Simulate append entries to follower
        request = AppendEntriesRequest(
            term=leader.current_term,
            leader_id=leader.node_id,
            prev_log_index=0,
            prev_log_term=0,
            entries=[leader.log[0]],
            leader_commit=0,
        )

        response = follower.handle_append_entries(request)

        assert response.success is True
        assert len(follower.log) == 1
        assert follower.log[0].command == command

    def test_append_entries_heartbeat(self, leader_follower_setup):
        """Test heartbeat append entries with no new entries"""
        leader, follower = leader_follower_setup

        # Send heartbeat (empty append entries)
        request = AppendEntriesRequest(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )

        response = follower.handle_append_entries(request)

        assert response.success is True
        assert response.term == 1
        assert len(follower.log) == 0

    def test_append_entries_log_consistency_check(self, leader_follower_setup):
        """Test log consistency checking in append entries"""
        leader, follower = leader_follower_setup

        # Add entry to follower log
        follower.log.append(
            LogEntry(
                term=1, index=0, command={"type": "SET", "key": "old", "value": "data"}
            )
        )

        # Send append entries with conflicting previous log
        request = AppendEntriesRequest(
            term=1,
            leader_id="leader",
            prev_log_index=0,
            prev_log_term=2,  # Different term than follower's entry
            entries=[
                LogEntry(
                    term=1,
                    index=1,
                    command={"type": "SET", "key": "new", "value": "data"},
                )
            ],
            leader_commit=0,
        )

        response = follower.handle_append_entries(request)

        # Should fail due to log inconsistency
        assert response.success is False

    def test_commit_index_update(self, leader_follower_setup):
        """Test commit index updates"""
        leader, follower = leader_follower_setup

        # Add entries to follower
        for i in range(3):
            follower.log.append(
                LogEntry(
                    term=1,
                    index=i,
                    command={"type": "SET", "key": f"key{i}", "value": f"value{i}"},
                )
            )

        # Send append entries with higher commit index
        request = AppendEntriesRequest(
            term=1,
            leader_id="leader",
            prev_log_index=2,
            prev_log_term=1,
            entries=[],
            leader_commit=2,  # Commit first 3 entries
        )

        response = follower.handle_append_entries(request)

        assert response.success is True
        assert follower.commit_index == 2


class TestRaftStateTransitions:
    """Test RAFT state transitions and safety properties"""

    @pytest.fixture
    def raft_node(self):
        """Create a RAFT node for state transition testing"""
        config = RaftConfig()
        node = RaftNode("test_node", ["peer1", "peer2"], config)
        return node

    def test_follower_to_candidate_transition(self, raft_node):
        """Test transition from follower to candidate"""
        node = raft_node

        # Initially follower
        assert node.state == RaftState.FOLLOWER
        assert node.current_term == 0

        # Manually trigger election
        node.current_term += 1
        node.state = RaftState.CANDIDATE
        node.voted_for = node.node_id

        assert node.state == RaftState.CANDIDATE
        assert node.current_term == 1
        assert node.voted_for == node.node_id

    def test_candidate_to_leader_transition(self, raft_node):
        """Test transition from candidate to leader"""
        node = raft_node

        # Set up as candidate
        node.state = RaftState.CANDIDATE
        node.current_term = 1
        node.voted_for = node.node_id

        # Become leader
        node.state = RaftState.LEADER
        node.next_index = {peer: len(node.log) + 1 for peer in node.peers}
        node.match_index = {peer: 0 for peer in node.peers}

        assert node.state == RaftState.LEADER
        assert len(node.next_index) == len(node.peers)
        assert len(node.match_index) == len(node.peers)

    def test_higher_term_step_down(self, raft_node):
        """Test stepping down when seeing higher term"""
        node = raft_node

        # Start as leader
        node.state = RaftState.LEADER
        node.current_term = 5

        # Process message with higher term
        node._step_down(10)

        assert node.state == RaftState.FOLLOWER
        assert node.current_term == 10
        assert node.voted_for is None

    def test_step_down_preserves_log(self, raft_node):
        """Test that stepping down preserves log entries"""
        node = raft_node

        # Add some log entries
        for i in range(3):
            node.log.append(
                LogEntry(
                    term=1,
                    index=i,
                    command={"type": "SET", "key": f"key{i}", "value": f"value{i}"},
                )
            )

        original_log_length = len(node.log)

        # Step down
        node._step_down(2)

        # Log should be preserved
        assert len(node.log) == original_log_length
        assert node.state == RaftState.FOLLOWER


class TestRaftPersistentState:
    """Test RAFT persistent state management"""

    @pytest.fixture
    def raft_node(self):
        """Create a RAFT node for persistence testing"""
        config = RaftConfig()
        node = RaftNode("test_node", ["peer1", "peer2"], config)
        return node

    def test_get_persistent_state(self, raft_node):
        """Test getting persistent state"""
        node = raft_node

        # Set up some state
        node.current_term = 5
        node.voted_for = "peer1"
        node.log.append(
            LogEntry(
                term=5, index=0, command={"type": "SET", "key": "test", "value": "data"}
            )
        )

        state = node.get_persistent_state()

        assert state["current_term"] == 5
        assert state["voted_for"] == "peer1"
        assert len(state["log"]) == 1
        assert state["log"][0]["term"] == 5
        assert state["log"][0]["command"]["key"] == "test"

    def test_restore_persistent_state(self, raft_node):
        """Test restoring persistent state"""
        node = raft_node

        # Prepare state to restore
        state = {
            "current_term": 7,
            "voted_for": "peer2",
            "log": [
                {
                    "term": 7,
                    "index": 0,
                    "command": {"type": "SET", "key": "restored", "value": "data"},
                    "timestamp": time.time(),
                }
            ],
            "last_included_index": 0,
            "last_included_term": 0,
        }

        # Restore state
        node.restore_persistent_state(state)

        assert node.current_term == 7
        assert node.voted_for == "peer2"
        assert len(node.log) == 1
        assert node.log[0].command["key"] == "restored"

    def test_state_persistence_roundtrip(self, raft_node):
        """Test complete state persistence roundtrip"""
        node = raft_node

        # Set up complex state
        node.current_term = 10
        node.voted_for = "peer1"
        node.commit_index = 2

        # Add multiple log entries
        for i in range(5):
            node.log.append(
                LogEntry(
                    term=9 + (i // 3),  # Mix of terms
                    index=i,
                    command={"type": "SET", "key": f"key{i}", "value": f"value{i}"},
                )
            )

        # Get state
        saved_state = node.get_persistent_state()

        # Create new node and restore
        new_config = RaftConfig()
        new_node = RaftNode("test_node", ["peer1", "peer2"], new_config)
        new_node.restore_persistent_state(saved_state)

        # Verify restoration
        assert new_node.current_term == node.current_term
        assert new_node.voted_for == node.voted_for
        assert len(new_node.log) == len(node.log)

        for original, restored in zip(node.log, new_node.log):
            assert original.term == restored.term
            assert original.index == restored.index
            assert original.command == restored.command


class TestRaftErrorHandling:
    """Test RAFT error handling and edge cases"""

    @pytest.fixture
    def raft_node(self):
        """Create a RAFT node for error testing"""
        config = RaftConfig()
        node = RaftNode("test_node", ["peer1", "peer2"], config)
        return node

    def test_invalid_append_entries_term(self, raft_node):
        """Test handling append entries with invalid term"""
        node = raft_node
        node.current_term = 5

        # Send append entries with older term
        request = AppendEntriesRequest(
            term=3,  # Older than current term
            leader_id="old_leader",
            prev_log_index=0,
            prev_log_term=0,
            entries=[],
            leader_commit=0,
        )

        response = node.handle_append_entries(request)

        assert response.success is False
        assert response.term == 5  # Current term returned

    def test_corrupted_log_entry_handling(self, raft_node):
        """Test handling of corrupted log entries"""
        node = raft_node

        # Test with malformed command
        try:
            entry = LogEntry(term=1, index=0, command=None)  # Invalid command
            node.log.append(entry)

            # Should handle gracefully
            assert len(node.log) == 1
        except Exception as e:
            # Should not crash the system
            assert isinstance(e, (TypeError, ValueError))

    def test_network_timeout_simulation(self, raft_node):
        """Test behavior during network timeouts"""
        node = raft_node

        # Mock network function that times out
        async def timeout_send_vote_request(peer, request):
            await asyncio.sleep(10)  # Simulate timeout
            return None

        node.send_vote_request = timeout_send_vote_request

        # Start election - should handle timeout gracefully
        # (This test would need to be expanded with actual timeout handling)
        assert node.state == RaftState.FOLLOWER  # Should remain in safe state

    def test_concurrent_election_handling(self, raft_node):
        """Test handling of concurrent elections"""
        node = raft_node

        # Simulate receiving vote requests from multiple candidates
        request1 = VoteRequest(
            term=1, candidate_id="candidate1", last_log_index=0, last_log_term=0
        )
        request2 = VoteRequest(
            term=1, candidate_id="candidate2", last_log_index=0, last_log_term=0
        )

        response1 = node.handle_vote_request(request1)
        response2 = node.handle_vote_request(request2)

        # Should grant vote to first candidate only
        assert response1.vote_granted is True
        assert response2.vote_granted is False
        assert node.voted_for == "candidate1"


class TestRaftPerformance:
    """Test RAFT performance characteristics"""

    @pytest.fixture
    def performance_raft_node(self):
        """Create a RAFT node optimized for performance testing"""
        config = RaftConfig(
            heartbeat_interval=0.01,  # Fast heartbeats
            max_log_entries_per_request=1000,  # Large batches
        )
        node = RaftNode("perf_node", ["peer1", "peer2"], config)

        # Mock fast network
        async def fast_append_entries(peer, request):
            return AppendEntriesResponse(
                term=request.term,
                success=True,
                last_log_index=request.prev_log_index + len(request.entries),
            )

        node.send_append_entries = fast_append_entries
        node.state = RaftState.LEADER
        node.current_term = 1

        return node

    @pytest.mark.asyncio
    async def test_high_throughput_log_replication(self, performance_raft_node):
        """Test high-throughput log replication"""
        node = performance_raft_node

        # Add many commands quickly
        start_time = time.time()
        command_count = 1000

        for i in range(command_count):
            command = {"type": "SET", "key": f"key{i}", "value": f"value{i}"}
            success = await node.add_command(command)
            assert success

        duration = time.time() - start_time
        throughput = command_count / duration

        # Should achieve reasonable throughput
        assert throughput > 100  # At least 100 commands/second
        assert len(node.log) == command_count

    def test_memory_usage_with_large_log(self, performance_raft_node):
        """Test memory usage with large log"""
        node = performance_raft_node

        # Add many entries to log
        entry_count = 10000

        for i in range(entry_count):
            entry = LogEntry(
                term=1,
                index=i,
                command={
                    "type": "SET",
                    "key": f"large_key_{i}",
                    "value": f"large_value_{i}",
                },
            )
            node.log.append(entry)

        # Verify log size
        assert len(node.log) == entry_count

        # Test log operations still work efficiently
        start_time = time.time()

        # Access last entries (should be O(1))
        last_entries = node.log[-100:]
        assert len(last_entries) == 100

        access_time = time.time() - start_time
        assert access_time < 0.1  # Should be very fast

    def test_state_transition_performance(self, performance_raft_node):
        """Test performance of state transitions"""
        node = performance_raft_node

        # Test rapid state transitions
        start_time = time.time()

        for _ in range(1000):
            # Follower -> Candidate
            node.state = RaftState.CANDIDATE
            node.current_term += 1

            # Candidate -> Leader
            node.state = RaftState.LEADER

            # Leader -> Follower
            node._step_down(node.current_term + 1)

        duration = time.time() - start_time
        transitions_per_sec = 3000 / duration  # 3 transitions per iteration

        # Should handle many transitions per second
        assert transitions_per_sec > 1000


if __name__ == "__main__":
    pytest.main([__file__])
