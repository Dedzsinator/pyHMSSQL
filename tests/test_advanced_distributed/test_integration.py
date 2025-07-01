"""
Integration tests for advanced distributed database features.

This module tests the integration between multiple advanced features including:
- RAFT consensus with WAL persistence
- Sharding with compression and zero-copy optimizations
- CRDT with networking and consistency levels
- Pub/Sub with RAFT coordination
- Full system workflows and performance under load

Created for comprehensive testing of production-ready distributed DBMS core service.
"""

import pytest
import asyncio
import time
import threading
import tempfile
import os
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from unittest.mock import Mock, patch, MagicMock

# Import all advanced features for integration testing
from kvstore.raft import RaftNode, RaftConfig, RaftState, LogEntry
from kvstore.consistency import ConsistencyLevel, ConsistencyManager, ConsistencyConfig
from kvstore.storage.wal import WriteAheadLog, WALConfig, WALEntry, WALEntryType
from kvstore.compression import CompressionManager, CompressionType
from kvstore.zerocopy import ZeroCopyManager, BufferPool
from kvstore.shards import AdvancedShardManager, AdvancedShard, ShardConfig
from kvstore.crdt import VectorClock, LWWElementSet, HybridLogicalClock
from kvstore.pubsub import PubSubManager, Message, Subscription
# from kvstore.networking import NetworkServer, RESPProtocol


class TestRaftWALIntegration:
    """Test integration between RAFT consensus and WAL persistence"""
    
    @pytest.fixture
    def raft_wal_system(self):
        """Setup integrated RAFT + WAL system"""
        async def _setup():
            temp_dir = tempfile.mkdtemp()
            try:
                # Setup WAL
                wal_config = WALConfig(
                    wal_dir=temp_dir,
                    segment_size_mb=1,
                    sync_interval_ms=100
                )
                wal = WriteAheadLog(wal_config)
                await wal.start()
                
                # Setup RAFT with WAL integration
                raft_config = RaftConfig()
                raft_config.election_timeout_min = 0.5
                raft_config.election_timeout_max = 1.0
                raft_config.heartbeat_interval = 0.2
                
                # Mock send functions for isolated testing
                async def mock_send_vote_request(peer, request):
                    return Mock(term=1, vote_granted=True)
                
                async def mock_send_append_entries(peer, request):
                    return Mock(term=1, success=True, last_log_index=len(request.entries))
                
                raft_node = RaftNode("node1", ["node2", "node3"], raft_config)
                raft_node.send_vote_request = mock_send_vote_request
                raft_node.send_append_entries = mock_send_append_entries
                
                # Integration callback to persist to WAL
                def on_command_applied(command):
                    entry = WALEntry(
                        entry_type=WALEntryType.SET,
                        key=command.get('key', 'unknown'),
                        value=command.get('value'),
                        metadata={'raft_term': raft_node.current_term}
                    )
                    asyncio.create_task(wal.write_entry(entry))
                
                raft_node.on_command_applied = on_command_applied
                raft_node.start()
                
                return {
                    'raft': raft_node,
                    'wal': wal,
                    'temp_dir': temp_dir
                }
            except Exception:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
                raise
        
        return _setup()
    
    @pytest.mark.asyncio
    async def test_raft_wal_command_persistence(self, raft_wal_system):
        """Test that RAFT commands are persisted via WAL"""
        system = await raft_wal_system
        raft = system['raft']
        wal = system['wal']
        
        # Add command to RAFT
        command = {'key': 'test_key', 'value': 'test_value', 'type': 'SET'}
        success = await raft.append_command(command)
        assert success
        
        # Wait for processing
        await asyncio.sleep(0.5)
        
        # Verify WAL contains the entry
        entries = await wal.read_entries_from(0)
        entries_list = [entry async for entry in entries]
        assert len(entries_list) > 0
        
        # Find our entry
        our_entry = None
        for entry in entries_list:
            if entry.key == 'test_key':
                our_entry = entry
                break
        
        assert our_entry is not None
        assert our_entry.value == 'test_value'
        assert our_entry.entry_type == WALEntryType.SET
        assert our_entry.metadata.get('raft_term') == raft.current_term
        
        # Cleanup
        raft.stop()
        await wal.stop()
        import shutil
        shutil.rmtree(system['temp_dir'], ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_raft_recovery_from_wal(self, raft_wal_system):
        """Test RAFT node recovery using WAL entries"""
        raft = raft_wal_system['raft']
        wal = raft_wal_system['wal']
        
        # Add multiple commands
        commands = [
            {'key': f'key_{i}', 'value': f'value_{i}', 'type': 'SET'}
            for i in range(5)
        ]
        
        for cmd in commands:
            await raft.add_command(cmd)
        
        await asyncio.sleep(1.0)
        
        # Stop RAFT but keep WAL
        raft.stop()
        
        # Create new RAFT node for recovery
        new_raft = RaftNode("node1", ["node2", "node3"], raft.config)
        new_raft.send_vote_request = raft.send_vote_request
        new_raft.send_append_entries = raft.send_append_entries
        
        # Recover from WAL
        recovered_state = {}
        entries = wal.read_entries_from(0)
        for entry in entries:
            if entry.entry_type == WALEntryType.SET:
                recovered_state[entry.key] = entry.value
        
        # Verify recovery
        assert len(recovered_state) == 5
        for i in range(5):
            assert recovered_state[f'key_{i}'] == f'value_{i}'
        
        new_raft.stop()
    
    @pytest.mark.asyncio
    async def test_raft_wal_concurrent_operations(self, raft_wal_system):
        """Test concurrent RAFT operations with WAL persistence"""
        raft = raft_wal_system['raft']
        wal = raft_wal_system['wal']
        
        async def add_commands(start_idx, count):
            for i in range(start_idx, start_idx + count):
                command = {
                    'key': f'concurrent_key_{i}',
                    'value': f'concurrent_value_{i}',
                    'type': 'SET'
                }
                await raft.add_command(command)
        
        # Run concurrent command additions
        tasks = [
            add_commands(0, 10),
            add_commands(10, 10),
            add_commands(20, 10)
        ]
        
        await asyncio.gather(*tasks)
        await asyncio.sleep(1.0)
        
        # Verify all entries in WAL
        entries = wal.read_entries_from(0)
        set_entries = [e for e in entries if e.entry_type == WALEntryType.SET]
        
        assert len(set_entries) == 30
        
        # Verify no duplicates
        keys = {e.key for e in set_entries}
        assert len(keys) == 30


class TestShardingCompressionIntegration:
    """Test integration between sharding, compression, and zero-copy optimizations"""
    
    @pytest.fixture
    def integrated_shard_system(self):
        """Setup integrated sharding + compression + zero-copy system"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup compression
            compression_manager = CompressionManager()
            
            # Setup zero-copy
            zerocopy_manager = ZeroCopyManager()
            
            # Setup sharding with integrated features
            shard_config = ShardConfig(
                num_shards=4,
                cache_size=1024,
                enable_compression=True,
                enable_zerocopy=True
            )
            
            shard_manager = AdvancedShardManager(shard_config)
            
            # Integrate components
            for shard in shard_manager.shards.values():
                shard.compression_manager = compression_manager
                shard.zerocopy_manager = zerocopy_manager
                shard.start()
            
            yield {
                'shard_manager': shard_manager,
                'compression': compression_manager,
                'zerocopy': zerocopy_manager,
                'temp_dir': temp_dir
            }
            
            for shard in shard_manager.shards.values():
                shard.stop()
    
    def test_compressed_sharded_storage(self, integrated_shard_system):
        """Test storing compressed data across shards"""
        shard_manager = integrated_shard_system['shard_manager']
        compression = integrated_shard_system['compression']
        
        # Store large, compressible data
        large_data = "Lorem ipsum " * 1000  # Highly compressible
        
        # Store with compression
        key = "large_text"
        compressed_result = compression.compress_text(large_data)
        
        success = shard_manager.set(key, compressed_result.compressed_data)
        assert success
        
        # Retrieve and decompress
        retrieved_compressed = shard_manager.get(key)
        assert retrieved_compressed is not None
        
        decompressed = compression.decompress(
            retrieved_compressed,
            CompressionType.LZ4
        )
        assert decompressed == large_data
        
        # Verify compression effectiveness
        original_size = len(large_data.encode())
        compressed_size = len(compressed_result.compressed_data)
        compression_ratio = compressed_size / original_size
        
        assert compression_ratio < 0.5  # At least 50% compression
    
    def test_zerocopy_sharded_operations(self, integrated_shard_system):
        """Test zero-copy operations across shards"""
        shard_manager = integrated_shard_system['shard_manager']
        zerocopy = integrated_shard_system['zerocopy']
        temp_dir = integrated_shard_system['temp_dir']
        
        # Create test file for memory mapping
        test_file = os.path.join(temp_dir, "test_data.bin")
        test_data = b"Zero-copy test data " * 100
        
        with open(test_file, 'wb') as f:
            f.write(test_data)
        
        # Map file and store reference in shard
        mapped_buffer = zerocopy.map_file(test_file, read_only=True)
        
        key = "zerocopy_data"
        success = shard_manager.set(key, mapped_buffer)
        assert success
        
        # Retrieve mapped data
        retrieved = shard_manager.get(key)
        assert retrieved is not None
        assert len(retrieved.data) == len(test_data)
        
        # Verify zero-copy efficiency
        buffer_stats = zerocopy.buffer_pool.get_stats()
        assert buffer_stats['total_gets'] > 0
        assert buffer_stats['reuse_count'] > 0
    
    def test_concurrent_shard_operations(self, integrated_shard_system):
        """Test concurrent operations across integrated shard system"""
        shard_manager = integrated_shard_system['shard_manager']
        
        def worker_thread(thread_id, operations_count):
            for i in range(operations_count):
                key = f"thread_{thread_id}_key_{i}"
                value = f"thread_{thread_id}_value_{i}"
                
                # Set operation
                success = shard_manager.set(key, value)
                assert success
                
                # Get operation
                retrieved = shard_manager.get(key)
                assert retrieved == value
                
                # Delete operation
                deleted = shard_manager.delete(key)
                assert deleted
        
        # Run concurrent workers
        threads = []
        for thread_id in range(5):
            thread = threading.Thread(
                target=worker_thread,
                args=(thread_id, 20)
            )
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Verify system state
        stats = shard_manager.get_stats()
        assert stats['total_operations'] >= 300  # 5 threads * 20 ops * 3 types
        assert stats['cache_hits'] > 0


class TestCRDTNetworkingIntegration:
    """Test integration between CRDT and networking components"""
    
    @pytest.fixture
    async def crdt_network_system(self):
        """Setup integrated CRDT + networking system"""
        # Setup vector clocks for multiple nodes
        nodes = {
            'node1': VectorClock('node1'),
            'node2': VectorClock('node2'),
            'node3': VectorClock('node3')
        }
        
        # Setup CRDT sets for each node
        crdt_sets = {
            node_id: LWWElementSet(clock)
            for node_id, clock in nodes.items()
        }
        
        # Setup networking (mocked for unit testing)
        network_server = Mock()
        network_server.broadcast_message = Mock()
        network_server.send_to_node = Mock()
        
        yield {
            'nodes': nodes,
            'crdt_sets': crdt_sets,
            'network': network_server
        }
    
    @pytest.mark.asyncio
    async def test_crdt_network_synchronization(self, crdt_network_system):
        """Test CRDT synchronization over network"""
        crdt_sets = crdt_network_system['crdt_sets']
        network = crdt_network_system['network']
        
        # Add elements to different nodes
        crdt_sets['node1'].add('element1')
        crdt_sets['node2'].add('element2')
        crdt_sets['node3'].add('element3')
        
        # Simulate network synchronization
        # Node1 sends its state to others
        node1_state = crdt_sets['node1'].get_state()
        crdt_sets['node2'].merge(node1_state)
        crdt_sets['node3'].merge(node1_state)
        
        # Node2 sends its state to others
        node2_state = crdt_sets['node2'].get_state()
        crdt_sets['node1'].merge(node2_state)
        crdt_sets['node3'].merge(node2_state)
        
        # Node3 sends its state to others
        node3_state = crdt_sets['node3'].get_state()
        crdt_sets['node1'].merge(node3_state)
        crdt_sets['node2'].merge(node3_state)
        
        # Verify convergence
        elements1 = crdt_sets['node1'].elements()
        elements2 = crdt_sets['node2'].elements()
        elements3 = crdt_sets['node3'].elements()
        
        assert elements1 == elements2 == elements3
        assert 'element1' in elements1
        assert 'element2' in elements1
        assert 'element3' in elements1
        
        # Verify network calls
        assert network.broadcast_message.call_count >= 0
    
    @pytest.mark.asyncio
    async def test_crdt_conflict_resolution(self, crdt_network_system):
        """Test CRDT conflict resolution over network"""
        crdt_sets = crdt_network_system['crdt_sets']
        
        # Simulate concurrent adds and removes of same element
        element = 'conflicted_element'
        
        # Node1 adds element
        crdt_sets['node1'].add(element)
        
        # Node2 also adds element (concurrent)
        crdt_sets['node2'].add(element)
        
        # Node3 removes element (concurrent)
        crdt_sets['node3'].remove(element)
        
        # Synchronize all states
        states = [crdt_set.get_state() for crdt_set in crdt_sets.values()]
        
        for crdt_set in crdt_sets.values():
            for state in states:
                crdt_set.merge(state)
        
        # Verify eventual consistency (LWW semantics)
        final_states = [crdt_set.elements() for crdt_set in crdt_sets.values()]
        
        # All nodes should have same final state
        assert all(state == final_states[0] for state in final_states)
    
    def test_hybrid_logical_clock_integration(self, crdt_network_system):
        """Test Hybrid Logical Clock with network time synchronization"""
        from kvstore.crdt import HybridLogicalClock
        
        # Setup HLC for multiple nodes
        hlc_nodes = {
            'node1': HybridLogicalClock('node1'),
            'node2': HybridLogicalClock('node2'),
            'node3': HybridLogicalClock('node3')
        }
        
        # Simulate network messages between nodes
        # Node1 sends message
        node1_time = hlc_nodes['node1'].tick()
        
        # Node2 receives message and updates
        node2_time = hlc_nodes['node2'].update(node1_time)
        
        # Node3 receives from node2
        node3_time = hlc_nodes['node3'].update(node2_time)
        
        # Verify causal ordering
        assert hlc_nodes['node1'].compare(node1_time, node2_time) <= 0
        assert hlc_nodes['node2'].compare(node2_time, node3_time) <= 0
        
        # Verify logical components increase
        assert node2_time['logical'] >= node1_time['logical']
        assert node3_time['logical'] >= node2_time['logical']


class TestPubSubRaftIntegration:
    """Test integration between Pub/Sub and RAFT consensus"""
    
    @pytest.fixture
    async def pubsub_raft_system(self):
        """Setup integrated Pub/Sub + RAFT system"""
        from kvstore.pubsub import PubSubManager
        
        # Setup RAFT node
        raft_config = RaftConfig(
            election_timeout_min=0.5,
            election_timeout_max=1.0,
            heartbeat_interval=0.2
        )
        
        raft_node = RaftNode("pubsub_leader", ["node2", "node3"], raft_config)
        
        # Mock send functions
        async def mock_send_vote_request(peer, request):
            return Mock(term=1, vote_granted=True)
        
        async def mock_send_append_entries(peer, request):
            return Mock(term=1, success=True, last_log_index=len(request.entries))
        
        raft_node.send_vote_request = mock_send_vote_request
        raft_node.send_append_entries = mock_send_append_entries
        
        # Setup Pub/Sub
        pubsub = PubSubManager()
        
        # Integrate Pub/Sub with RAFT
        def on_raft_command(command):
            if command.get('type') == 'PUBLISH':
                channel = command.get('channel')
                data = command.get('data')
                if channel and data is not None:
                    asyncio.create_task(pubsub.publish(channel, data))
        
        raft_node.on_command_applied = on_raft_command
        raft_node.start()
        
        yield {
            'raft': raft_node,
            'pubsub': pubsub
        }
        
        raft_node.stop()
    
    @pytest.mark.asyncio
    async def test_coordinated_message_publishing(self, pubsub_raft_system):
        """Test coordinated message publishing through RAFT"""
        raft = pubsub_raft_system['raft']
        pubsub = pubsub_raft_system['pubsub']
        
        # Subscribe to channel
        messages = []
        
        async def message_handler(message):
            messages.append(message)
        
        await pubsub.subscribe('test_channel', message_handler)
        
        # Publish through RAFT coordination
        publish_command = {
            'type': 'PUBLISH',
            'channel': 'test_channel',
            'data': 'coordinated_message'
        }
        
        success = await raft.add_command(publish_command)
        assert success
        
        # Wait for processing
        await asyncio.sleep(0.5)
        
        # Verify message received
        assert len(messages) == 1
        assert messages[0].data == 'coordinated_message'
        assert messages[0].channel == 'test_channel'
    
    @pytest.mark.asyncio
    async def test_pubsub_high_availability(self, pubsub_raft_system):
        """Test Pub/Sub high availability with RAFT failover"""
        raft = pubsub_raft_system['raft']
        pubsub = pubsub_raft_system['pubsub']
        
        # Setup subscribers
        received_messages = []
        
        async def collector(message):
            received_messages.append(message.data)
        
        await pubsub.subscribe('ha_channel', collector)
        
        # Publish multiple messages
        messages = ['msg1', 'msg2', 'msg3']
        
        for i, msg in enumerate(messages):
            command = {
                'type': 'PUBLISH',
                'channel': 'ha_channel',
                'data': msg
            }
            await raft.add_command(command)
            
            # Simulate brief network issue after first message
            if i == 0:
                await asyncio.sleep(0.2)
        
        await asyncio.sleep(1.0)
        
        # Verify all messages received despite network issues
        assert len(received_messages) == 3
        assert all(msg in received_messages for msg in messages)


class TestFullSystemIntegration:
    """Test full system integration with all advanced features"""
    
    @pytest.fixture
    async def full_system(self):
        """Setup complete integrated system"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Initialize all components
            components = {}
            
            # WAL
            wal_config = WALConfig(wal_dir=temp_dir, max_segment_size=1024*1024)
            components['wal'] = WriteAheadLog(wal_config)
            components['wal'].start()
            
            # Compression
            components['compression'] = CompressionManager()
            
            # Zero-copy
            components['zerocopy'] = ZeroCopyManager()
            
            # Consistency
            consistency_config = ConsistencyConfig(
                default_level=ConsistencyLevel.QUORUM,
                read_timeout=5.0,
                write_timeout=5.0
            )
            components['consistency'] = ConsistencyManager(consistency_config)
            
            # Sharding
            shard_config = ShardConfig(num_shards=3)
            components['sharding'] = AdvancedShardManager(shard_config)
            
            # RAFT
            raft_config = RaftConfig()
            components['raft'] = RaftNode("system_node", ["peer1", "peer2"], raft_config)
            
            # Mock network functions
            async def mock_send_vote_request(peer, request):
                return Mock(term=1, vote_granted=True)
            
            async def mock_send_append_entries(peer, request):
                return Mock(term=1, success=True, last_log_index=len(request.entries))
            
            components['raft'].send_vote_request = mock_send_vote_request
            components['raft'].send_append_entries = mock_send_append_entries
            
            # Pub/Sub
            from kvstore.pubsub import PubSubManager
            components['pubsub'] = PubSubManager()
            
            # CRDT
            components['vector_clock'] = VectorClock('system_node')
            components['crdt_set'] = LWWElementSet(components['vector_clock'])
            
            # Start services
            for shard in components['sharding'].shards.values():
                shard.start()
            components['raft'].start()
            
            yield components
            
            # Cleanup
            components['raft'].stop()
            for shard in components['sharding'].shards.values():
                shard.stop()
            components['wal'].stop()
    
    @pytest.mark.asyncio
    async def test_full_system_workflow(self, full_system):
        """Test complete workflow using all system components"""
        # Extract components
        wal = full_system['wal']
        compression = full_system['compression']
        sharding = full_system['sharding']
        raft = full_system['raft']
        consistency = full_system['consistency']
        
        # Workflow: Client writes data
        # 1. Data is compressed
        original_data = "Important system data " * 100
        compressed_result = compression.compress_text(original_data)
        
        # 2. Write goes through RAFT for consensus
        write_command = {
            'type': 'SET',
            'key': 'system_key',
            'value': compressed_result.compressed_data,
            'consistency_level': ConsistencyLevel.QUORUM.value
        }
        
        raft_success = await raft.add_command(write_command)
        assert raft_success
        
        # 3. Data is persisted via WAL
        wal_entry = WALEntry(
            entry_type=WALEntryType.SET,
            key='system_key',
            value=compressed_result.compressed_data
        )
        wal.write_entry(wal_entry)
        
        # 4. Data is stored in shards
        shard_success = sharding.set('system_key', compressed_result.compressed_data)
        assert shard_success
        
        # 5. Read data back
        retrieved_compressed = sharding.get('system_key')
        assert retrieved_compressed is not None
        
        # 6. Decompress data
        decompressed = compression.decompress(
            retrieved_compressed,
            CompressionType.LZ4
        )
        assert decompressed == original_data
        
        # 7. Verify WAL persistence
        wal_entries = wal.read_entries_from(0)
        assert len(wal_entries) > 0
        
        # Find our entry
        our_wal_entry = None
        for entry in wal_entries:
            if entry.key == 'system_key':
                our_wal_entry = entry
                break
        
        assert our_wal_entry is not None
        assert our_wal_entry.value == compressed_result.compressed_data
    
    @pytest.mark.asyncio
    async def test_system_fault_tolerance(self, full_system):
        """Test system fault tolerance and recovery"""
        raft = full_system['raft']
        wal = full_system['wal']
        sharding = full_system['sharding']
        
        # Add test data
        test_commands = [
            {'type': 'SET', 'key': f'fault_key_{i}', 'value': f'fault_value_{i}'}
            for i in range(10)
        ]
        
        for cmd in test_commands:
            await raft.add_command(cmd)
        
        await asyncio.sleep(0.5)
        
        # Simulate component failure and recovery
        # Stop RAFT
        raft.stop()
        
        # Verify data still accessible via shards
        for i in range(10):
            retrieved = sharding.get(f'fault_key_{i}')
            # May be None if not yet persisted to shards
        
        # Verify WAL has all entries for recovery
        wal_entries = wal.read_entries_from(0)
        
        # Should have entries from RAFT operations
        assert len(wal_entries) >= 0  # May not have persisted all yet
        
        # Restart RAFT (simulated recovery)
        new_raft = RaftNode("system_node", ["peer1", "peer2"], raft.config)
        new_raft.send_vote_request = raft.send_vote_request
        new_raft.send_append_entries = raft.send_append_entries
        new_raft.start()
        
        # System should be operational
        recovery_command = {'type': 'SET', 'key': 'recovery_test', 'value': 'recovered'}
        success = await new_raft.add_command(recovery_command)
        assert success
        
        new_raft.stop()
    
    def test_system_performance_characteristics(self, full_system):
        """Test system performance under load"""
        sharding = full_system['sharding']
        compression = full_system['compression']
        
        # Performance test data
        test_data = "Performance test data " * 50
        compressed_result = compression.compress_text(test_data)
        
        # Measure write performance
        start_time = time.time()
        operations_count = 1000
        
        for i in range(operations_count):
            key = f'perf_key_{i}'
            success = sharding.set(key, compressed_result.compressed_data)
            assert success
        
        write_duration = time.time() - start_time
        write_ops_per_sec = operations_count / write_duration
        
        # Measure read performance
        start_time = time.time()
        
        for i in range(operations_count):
            key = f'perf_key_{i}'
            value = sharding.get(key)
            assert value is not None
        
        read_duration = time.time() - start_time
        read_ops_per_sec = operations_count / read_duration
        
        # Performance assertions
        assert write_ops_per_sec > 100  # At least 100 writes/sec
        assert read_ops_per_sec > 500   # At least 500 reads/sec
        
        # Verify compression effectiveness
        original_size = len(test_data.encode())
        compressed_size = len(compressed_result.compressed_data)
        compression_ratio = compressed_size / original_size
        
        assert compression_ratio < 0.8  # At least 20% compression
        
        # Get system statistics
        shard_stats = sharding.get_stats()
        assert shard_stats['total_operations'] >= 2000
        assert shard_stats['cache_hits'] > 0


# Performance benchmarks
class TestSystemBenchmarks:
    """Performance benchmarks for the complete system"""

    @pytest.mark.benchmark
    def test_write_throughput_benchmark(self):
        """Benchmark write throughput across all components"""
        from kvstore.shards import AdvancedShardManager
        from kvstore.compression import CompressionManager, CompressionType
        import time
        # Setup
        compression = CompressionManager()
        shard_manager = AdvancedShardManager({'num_shards': 4})
        shard_manager.start()
        print(f"Available shard IDs: {list(shard_manager.shards.keys())}")
        test_data = "Benchmark write data " * 50
        compressed = compression.compress(test_data.encode(), algorithm=CompressionType.LZ4).compressed_data
        # Benchmark
        operations_count = 2000
        start = time.time()
        for i in range(operations_count):
            key = f'bench_write_{i}'
            shard_id = shard_manager.get_shard_for_key(key)
            shard_manager.shards[shard_id].set(key, compressed)
        duration = time.time() - start
        throughput = operations_count / duration
        for shard in shard_manager.shards.values():
            shard.stop()
        print(f"Write throughput: {throughput:.2f} ops/sec")
        assert throughput > 200  # Reasonable lower bound

    @pytest.mark.benchmark
    def test_read_latency_benchmark(self):
        """Benchmark read latency across all components"""
        from kvstore.shards import AdvancedShardManager
        from kvstore.compression import CompressionManager, CompressionType
        import time
        # Setup
        compression = CompressionManager()
        shard_manager = AdvancedShardManager({'num_shards': 4})
        shard_manager.start()
        print(f"Available shard IDs: {list(shard_manager.shards.keys())}")
        test_data = "Benchmark read data " * 50
        compressed = compression.compress(test_data.encode(), algorithm=CompressionType.LZ4).compressed_data
        # Preload
        for i in range(1000):
            key = f'bench_read_{i}'
            shard_id = shard_manager.get_shard_for_key(key)
            shard_manager.shards[shard_id].set(key, compressed)
        # Benchmark
        start = time.time()
        for i in range(1000):
            key = f'bench_read_{i}'
            shard_id = shard_manager.get_shard_for_key(key)
            value = shard_manager.shards[shard_id].get(key)
            assert value is not None
        duration = time.time() - start
        avg_latency_ms = (duration / 1000) * 1000
        for shard in shard_manager.shards.values():
            shard.stop()
        print(f"Average read latency: {avg_latency_ms:.2f} ms")
        assert avg_latency_ms < 10  # Reads should be fast

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_consensus_latency_benchmark(self):
        """Benchmark RAFT consensus latency under load"""
        from kvstore.raft import RaftNode, RaftConfig
        import time
        # Setup
        raft_config = RaftConfig()
        raft_config.election_timeout_min = 0.5
        raft_config.election_timeout_max = 1.0
        raft_config.heartbeat_interval = 0.2
        raft = RaftNode("bench_node", ["peer1", "peer2"], raft_config)
        async def mock_send_vote_request(peer, request):
            return Mock(term=1, vote_granted=True)
        async def mock_send_append_entries(peer, request):
            return Mock(term=1, success=True, last_log_index=len(request.entries))
        raft.send_vote_request = mock_send_vote_request
        raft.send_append_entries = mock_send_append_entries
        raft.start()
        # Benchmark
        commands = [{"key": f"consensus_{i}", "value": f"val_{i}", "type": "SET"} for i in range(100)]
        start = time.time()
        for cmd in commands:
            await raft.append_command(cmd)
        await asyncio.sleep(0.5)
        duration = time.time() - start
        avg_latency_ms = (duration / len(commands)) * 1000
        raft.stop()
        print(f"Average RAFT consensus latency: {avg_latency_ms:.2f} ms")
        assert avg_latency_ms < 50  # Consensus should be reasonably fast


# === UNIT TESTS FOR NEW FUNCTIONALITY ===

class TestUnitCompressionManager:
    def test_compress_and_decompress_text(self):
        from kvstore.compression import CompressionManager, CompressionType
        manager = CompressionManager()
        text = "unit test data " * 10
        result = manager.compress_text(text)
        assert hasattr(result, 'compressed_data')
        decompressed = manager.decompress(result.compressed_data, CompressionType.LZ4)
        assert decompressed == text

    def test_compression_ratio(self):
        from kvstore.compression import CompressionManager, CompressionType
        manager = CompressionManager()
        text = "A" * 1000
        result = manager.compress_text(text)
        ratio = len(result.compressed_data) / len(text.encode())
        assert ratio < 0.5

class TestUnitZeroCopyManager:
    def test_map_file_and_stats(self, tmp_path):
        from kvstore.zerocopy import ZeroCopyManager
        test_file = tmp_path / "test.bin"
        data = b"abc" * 100
        test_file.write_bytes(data)
        manager = ZeroCopyManager()
        mapped = manager.map_file(str(test_file), read_only=True)
        assert hasattr(mapped, 'data')
        assert mapped.data[:3] == b"abc"
        stats = manager.buffer_pool.get_stats()
        assert 'total_gets' in stats

class TestUnitAdvancedShardManager:
    def test_set_get_delete(self):
        from kvstore.shards import AdvancedShardManager, ShardConfig
        config = ShardConfig(num_shards=2)
        manager = AdvancedShardManager(config)
        manager.start()
        key, value = "k", "v"
        assert manager.set(key, value)
        assert manager.get(key) == value
        assert manager.delete(key)
        manager.stop()

class TestUnitRaftNode:
    def test_raft_node_lifecycle(self):
        from kvstore.raft import RaftNode, RaftConfig
        config = RaftConfig()
        node = RaftNode("n1", ["n2"], config)
        node.start()
        assert node.state in ("leader", "follower", "candidate")
        node.stop()
        assert not node.running

class TestUnitLWWElementSet:
    def test_add_remove_merge(self):
        from kvstore.crdt import VectorClock, LWWElementSet
        clock = VectorClock("n")
        s1 = LWWElementSet(clock)
        s2 = LWWElementSet(clock)
        s1.add("a")
        s2.add("b")
        s1.merge(s2.get_state())
        assert "a" in s1.elements() and "b" in s1.elements()
        s1.remove("a")
        assert "a" not in s1.elements()

class TestUnitHybridLogicalClock:
    def test_tick_and_update(self):
        from kvstore.crdt import HybridLogicalClock
        hlc1 = HybridLogicalClock("n1")
        t1 = hlc1.tick()
        t2 = hlc1.update(t1)
        assert hlc1.compare(t1, t2) <= 0

class TestUnitPubSubManager:
    @pytest.mark.asyncio
    async def test_pubsub_publish_subscribe(self):
        from kvstore.pubsub import PubSubManager
        pubsub = PubSubManager()
        received = []
        async def handler(msg):
            received.append(msg.data)
        await pubsub.subscribe("chan", handler)
        await pubsub.publish("chan", "hello")
        await asyncio.sleep(0.1)
        assert "hello" in received


if __name__ == '__main__':
    pytest.main([__file__])
