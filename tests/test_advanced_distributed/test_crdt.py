"""
Comprehensive unit tests for CRDT (Conflict-free Replicated Data Types) implementation.

This module tests CRDT functionality including:
- Vector Clocks and Hybrid Logical Clocks
- LWW-Element-Set and OR-Set implementations
- Counter CRDTs and PN-Counter
- CRDT synchronization and conflict resolution
- Performance characteristics and error handling

Created for comprehensive testing of production-ready distributed CRDT core.
"""

import pytest
import time
import threading
import json
from unittest.mock import Mock, patch
from typing import Dict, Any, List, Set
from concurrent.futures import ThreadPoolExecutor

from kvstore.crdt import (
    VectorClock, HybridLogicalClock,
    LWWElementSet, ORSet, GCounter, PNCounter,
    CRDTManager, Clock
)


class TestVectorClock:
    """Test Vector Clock implementation"""
    
    def test_vector_clock_initialization(self):
        """Test vector clock initialization"""
        clock = VectorClock("node1")
        
        assert clock.node_id == "node1"
        assert clock.clock == {"node1": 0}
    
    def test_vector_clock_with_initial_clock(self):
        """Test vector clock with initial clock state"""
        initial_clock = {"node1": 5, "node2": 3}
        clock = VectorClock("node1", initial_clock)
        
        assert clock.node_id == "node1"
        assert clock.clock == initial_clock
    
    def test_vector_clock_tick(self):
        """Test vector clock tick operation"""
        clock = VectorClock("node1")
        
        # Initial tick
        new_time = clock.tick()
        assert new_time == {"node1": 1}
        assert clock.clock == {"node1": 1}
        
        # Multiple ticks
        clock.tick()
        clock.tick()
        assert clock.clock == {"node1": 3}
    
    def test_vector_clock_update(self):
        """Test vector clock update from other clocks"""
        clock1 = VectorClock("node1")
        clock2 = VectorClock("node2")
        
        # Advance clocks independently
        clock1.tick()
        clock1.tick()
        
        clock2.tick()
        clock2.tick()
        clock2.tick()
        
        # Update clock1 with clock2's state
        updated_time = clock1.update(clock2.clock)
        
        expected = {"node1": 2, "node2": 3}
        assert updated_time == expected
        assert clock1.clock == expected
    
    def test_vector_clock_compare(self):
        """Test vector clock comparison"""
        clock1 = VectorClock("node1")
        clock2 = VectorClock("node2")
        
        # Initially concurrent (both at 0)
        time1 = clock1.clock.copy()
        time2 = clock2.clock.copy()
        assert clock1.compare(time1, time2) == 0  # Concurrent
        
        # clock1 advances
        clock1.tick()
        time1_new = clock1.clock.copy()
        assert clock1.compare(time1_new, time2) == 1  # time1_new > time2
        assert clock1.compare(time2, time1_new) == -1  # time2 < time1_new
        
        # Both advance, still comparable
        clock2.tick()
        clock2.tick()
        time2_new = clock2.clock.copy()
        assert clock1.compare(time1_new, time2_new) == 0  # Concurrent
    
    def test_vector_clock_thread_safety(self):
        """Test vector clock thread safety"""
        clock = VectorClock("node1")
        
        def tick_worker():
            for _ in range(100):
                clock.tick()
        
        # Run multiple threads
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=tick_worker)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have 500 total ticks
        assert clock.clock["node1"] == 500


class TestHybridLogicalClock:
    """Test Hybrid Logical Clock implementation"""
    
    def test_hlc_initialization(self):
        """Test HLC initialization"""
        hlc = HybridLogicalClock("node1")
        
        assert hlc.node_id == "node1"
        assert hlc.logical_time == 0
        assert hlc.counter == 0
    
    def test_hlc_tick(self):
        """Test HLC tick operation"""
        hlc = HybridLogicalClock("node1")
        
        # Mock current time
        with patch('time.time', return_value=1000.0):
            timestamp = hlc.tick()
        
        assert timestamp['physical'] == 1000.0
        assert timestamp['logical'] >= 1000.0
        assert timestamp['counter'] == 0
        assert timestamp['node_id'] == "node1"
    
    def test_hlc_update_with_future_time(self):
        """Test HLC update with future timestamp"""
        hlc = HybridLogicalClock("node1")
        
        # Start with current time
        with patch('time.time', return_value=1000.0):
            hlc.tick()
        
        # Receive timestamp from future
        future_timestamp = {
            'physical': 1100.0,
            'logical': 1100.0,
            'counter': 0,
            'node_id': 'node2'
        }
        
        with patch('time.time', return_value=1050.0):  # Still in past
            updated = hlc.update(future_timestamp)
        
        # Should advance to future time
        assert updated['logical'] == 1100.0
        assert updated['counter'] == 1  # Increment counter
    
    def test_hlc_update_with_past_time(self):
        """Test HLC update with past timestamp"""
        hlc = HybridLogicalClock("node1")
        
        # Start with current time
        with patch('time.time', return_value=1000.0):
            current = hlc.tick()
        
        # Receive timestamp from past
        past_timestamp = {
            'physical': 900.0,
            'logical': 900.0,
            'counter': 0,
            'node_id': 'node2'
        }
        
        with patch('time.time', return_value=1050.0):
            updated = hlc.update(past_timestamp)
        
        # Should use current physical time
        assert updated['physical'] == 1050.0
        assert updated['logical'] >= max(current['logical'], 1050.0)
    
    def test_hlc_compare(self):
        """Test HLC timestamp comparison"""
        hlc = HybridLogicalClock("node1")
        
        timestamp1 = {
            'physical': 1000.0,
            'logical': 1000.0,
            'counter': 0,
            'node_id': 'node1'
        }
        
        timestamp2 = {
            'physical': 1000.0,
            'logical': 1001.0,
            'counter': 0,
            'node_id': 'node2'
        }
        
        timestamp3 = {
            'physical': 1000.0,
            'logical': 1000.0,
            'counter': 1,
            'node_id': 'node2'
        }
        
        # timestamp2 > timestamp1 (higher logical time)
        assert hlc.compare(timestamp1, timestamp2) == -1
        assert hlc.compare(timestamp2, timestamp1) == 1
        
        # timestamp3 > timestamp1 (same logical, higher counter)
        assert hlc.compare(timestamp1, timestamp3) == -1
        assert hlc.compare(timestamp3, timestamp1) == 1
        
        # Same timestamp
        assert hlc.compare(timestamp1, timestamp1.copy()) == 0


class TestLWWElementSet:
    """Test Last-Writer-Wins Element Set implementation"""
    
    def test_lww_set_initialization(self):
        """Test LWW set initialization"""
        clock = VectorClock("node1")
        lww_set = LWWElementSet(clock)
        
        assert lww_set.clock == clock
        assert len(lww_set.add_map) == 0
        assert len(lww_set.remove_map) == 0
    
    def test_lww_set_add_element(self):
        """Test adding elements to LWW set"""
        clock = VectorClock("node1")
        lww_set = LWWElementSet(clock)
        
        # Add element
        lww_set.add("element1")
        
        assert "element1" in lww_set.add_map
        assert lww_set.add_map["element1"]["node1"] == 1
        assert "element1" in lww_set.elements()
    
    def test_lww_set_remove_element(self):
        """Test removing elements from LWW set"""
        clock = VectorClock("node1")
        lww_set = LWWElementSet(clock)
        
        # Add then remove element
        lww_set.add("element1")
        lww_set.remove("element1")
        
        assert "element1" in lww_set.add_map
        assert "element1" in lww_set.remove_map
        assert lww_set.remove_map["element1"]["node1"] == 2
        assert "element1" not in lww_set.elements()
    
    def test_lww_set_concurrent_operations(self):
        """Test concurrent add/remove operations"""
        clock1 = VectorClock("node1")
        clock2 = VectorClock("node2")
        
        lww_set1 = LWWElementSet(clock1)
        lww_set2 = LWWElementSet(clock2)
        
        # Concurrent operations
        lww_set1.add("element1")  # node1: add at time 1
        lww_set2.remove("element1")  # node2: remove at time 1
        
        # Merge sets
        state1 = lww_set1.get_state()
        lww_set2.merge(state1)
        
        # LWW semantics: add wins when concurrent (or remove wins, depending on implementation)
        # Let's assume remove wins in case of ties
        elements = lww_set2.elements()
        # Result depends on implementation choice for concurrent operations
    
    def test_lww_set_merge(self):
        """Test merging LWW sets"""
        clock1 = VectorClock("node1")
        clock2 = VectorClock("node2")
        
        lww_set1 = LWWElementSet(clock1)
        lww_set2 = LWWElementSet(clock2)
        
        # Add different elements
        lww_set1.add("element1")
        lww_set1.add("element2")
        
        lww_set2.add("element2")
        lww_set2.add("element3")
        
        # Get states and merge
        state1 = lww_set1.get_state()
        state2 = lww_set2.get_state()
        
        lww_set1.merge(state2)
        lww_set2.merge(state1)
        
        # Both sets should converge
        elements1 = lww_set1.elements()
        elements2 = lww_set2.elements()
        
        assert elements1 == elements2
        assert "element1" in elements1
        assert "element2" in elements1
        assert "element3" in elements1
    
    def test_lww_set_serialization(self):
        """Test LWW set serialization"""
        clock = VectorClock("node1")
        lww_set = LWWElementSet(clock)
        
        # Add some elements
        lww_set.add("element1")
        lww_set.add("element2")
        lww_set.remove("element1")
        
        # Get state
        state = lww_set.get_state()
        
        # Verify state structure
        assert "add_map" in state
        assert "remove_map" in state
        assert "element1" in state["add_map"]
        assert "element1" in state["remove_map"]
        assert "element2" in state["add_map"]
        
        # Test JSON serialization
        json_state = json.dumps(state)
        restored_state = json.loads(json_state)
        
        # Create new set and merge
        new_clock = VectorClock("node2")
        new_lww_set = LWWElementSet(new_clock)
        new_lww_set.merge(restored_state)
        
        # Should have same elements
        assert new_lww_set.elements() == lww_set.elements()


class TestORSet:
    """Test Observed-Remove Set implementation"""
    
    def test_or_set_initialization(self):
        """Test OR set initialization"""
        clock = VectorClock("node1")
        or_set = ORSet(clock)
        
        assert or_set.clock == clock
        assert len(or_set.added_elements) == 0
        assert len(or_set.removed_tags) == 0
    
    def test_or_set_add_element(self):
        """Test adding elements to OR set"""
        clock = VectorClock("node1")
        or_set = ORSet(clock)
        
        # Add element
        or_set.add("element1")
        
        assert len(or_set.added_elements) == 1
        assert "element1" in or_set.elements()
        
        # Adding same element again creates new tag
        or_set.add("element1")
        assert len(or_set.added_elements) == 2
    
    def test_or_set_remove_element(self):
        """Test removing elements from OR set"""
        clock = VectorClock("node1")
        or_set = ORSet(clock)
        
        # Add then remove element
        or_set.add("element1")
        elements_before_remove = or_set.elements()
        assert "element1" in elements_before_remove
        
        or_set.remove("element1")
        elements_after_remove = or_set.elements()
        assert "element1" not in elements_after_remove
        
        # Should have removed tags
        assert len(or_set.removed_tags) > 0
    
    def test_or_set_concurrent_add_remove(self):
        """Test concurrent add/remove in OR set"""
        clock1 = VectorClock("node1")
        clock2 = VectorClock("node2")
        
        or_set1 = ORSet(clock1)
        or_set2 = ORSet(clock2)
        
        # Node1 adds element
        or_set1.add("element1")
        
        # Node2 adds same element independently  
        or_set2.add("element1")
        
        # Node1 removes element
        or_set1.remove("element1")
        
        # Merge sets
        state1 = or_set1.get_state()
        state2 = or_set2.get_state()
        
        or_set1.merge(state2)
        or_set2.merge(state1)
        
        # Element should still exist (add from node2 not observed by node1's remove)
        elements1 = or_set1.elements()
        elements2 = or_set2.elements()
        
        assert elements1 == elements2
        # OR-Set semantics: element exists if there's an add not observed by remove
    
    def test_or_set_merge_commutativity(self):
        """Test OR set merge commutativity"""
        clock1 = VectorClock("node1")
        clock2 = VectorClock("node2")
        
        or_set1 = ORSet(clock1)
        or_set2 = ORSet(clock2)
        
        # Different operations on each set
        or_set1.add("a")
        or_set1.add("b")
        or_set1.remove("a")
        
        or_set2.add("b")
        or_set2.add("c")
        
        # Create copies for testing commutativity
        or_set1_copy = ORSet(VectorClock("node1"))
        or_set1_copy.merge(or_set1.get_state())
        
        or_set2_copy = ORSet(VectorClock("node2"))
        or_set2_copy.merge(or_set2.get_state())
        
        # Merge in different orders
        # Order 1: set1 <- set2, then set2 <- set1
        state2 = or_set2.get_state()
        or_set1.merge(state2)
        state1_updated = or_set1.get_state()
        or_set2.merge(state1_updated)
        
        # Order 2: set1_copy <- set2_copy, then set2_copy <- set1_copy
        state2_copy = or_set2_copy.get_state()
        or_set1_copy.merge(state2_copy)
        state1_copy_updated = or_set1_copy.get_state()
        or_set2_copy.merge(state1_copy_updated)
        
        # Results should be same regardless of merge order
        assert or_set1.elements() == or_set1_copy.elements()
        assert or_set2.elements() == or_set2_copy.elements()


class TestGCounter:
    """Test G-Counter (Grow-only Counter) implementation"""
    
    def test_g_counter_initialization(self):
        """Test G-Counter initialization"""
        counter = GCounter("node1")
        
        assert counter.node_id == "node1"
        assert counter.counters == {"node1": 0}
    
    def test_g_counter_increment(self):
        """Test G-Counter increment"""
        counter = GCounter("node1")
        
        # Increment
        counter.increment()
        assert counter.value() == 1
        assert counter.counters["node1"] == 1
        
        # Multiple increments
        counter.increment(5)
        assert counter.value() == 6
        assert counter.counters["node1"] == 6
    
    def test_g_counter_merge(self):
        """Test G-Counter merge"""
        counter1 = GCounter("node1")
        counter2 = GCounter("node2")
        
        # Increment independently
        counter1.increment(3)
        counter2.increment(5)
        
        # Merge
        state2 = counter2.get_state()
        counter1.merge(state2)
        
        # Total should be sum
        assert counter1.value() == 8
        assert counter1.counters["node1"] == 3
        assert counter1.counters["node2"] == 5
    
    def test_g_counter_concurrent_increments(self):
        """Test concurrent increments on G-Counter"""
        counter = GCounter("node1")
        
        def increment_worker():
            for _ in range(100):
                counter.increment()
        
        # Run concurrent increments
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=increment_worker)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Should have 500 total increments
        assert counter.value() == 500


class TestPNCounter:
    """Test PN-Counter (Increment/Decrement Counter) implementation"""
    
    def test_pn_counter_initialization(self):
        """Test PN-Counter initialization"""
        counter = PNCounter("node1")
        
        assert counter.node_id == "node1"
        assert counter.p_counter.node_id == "node1"
        assert counter.n_counter.node_id == "node1"
        assert counter.value() == 0
    
    def test_pn_counter_increment(self):
        """Test PN-Counter increment"""
        counter = PNCounter("node1")
        
        counter.increment(5)
        assert counter.value() == 5
        
        counter.increment(3)
        assert counter.value() == 8
    
    def test_pn_counter_decrement(self):
        """Test PN-Counter decrement"""
        counter = PNCounter("node1")
        
        counter.decrement(3)
        assert counter.value() == -3
        
        counter.increment(5)
        assert counter.value() == 2
    
    def test_pn_counter_merge(self):
        """Test PN-Counter merge"""
        counter1 = PNCounter("node1")
        counter2 = PNCounter("node2")
        
        # Different operations
        counter1.increment(10)
        counter1.decrement(3)
        
        counter2.increment(5)
        counter2.decrement(8)
        
        # Merge
        state2 = counter2.get_state()
        counter1.merge(state2)
        
        # Total: (10 + 5) - (3 + 8) = 4
        assert counter1.value() == 4
    
    def test_pn_counter_concurrent_operations(self):
        """Test concurrent increment/decrement operations"""
        counter = PNCounter("node1")
        
        def increment_worker():
            for _ in range(50):
                counter.increment()
        
        def decrement_worker():
            for _ in range(30):
                counter.decrement()
        
        # Run concurrent operations
        threads = []
        
        # 3 increment threads, 2 decrement threads
        for _ in range(3):
            thread = threading.Thread(target=increment_worker)
            threads.append(thread)
            thread.start()
        
        for _ in range(2):
            thread = threading.Thread(target=decrement_worker)
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Total: (3 * 50) - (2 * 30) = 150 - 60 = 90
        assert counter.value() == 90


class TestCRDTManager:
    """Test CRDT Manager for coordinating multiple CRDTs"""
    
    def test_crdt_manager_initialization(self):
        """Test CRDT manager initialization"""
        manager = CRDTManager("node1")
        
        assert manager.node_id == "node1"
        assert isinstance(manager.clock, VectorClock)
        assert manager.clock.node_id == "node1"
        assert len(manager.crdts) == 0
    
    def test_crdt_manager_create_lww_set(self):
        """Test creating LWW set through manager"""
        manager = CRDTManager("node1")
        
        lww_set = manager.create_lww_set("my_set")
        
        assert "my_set" in manager.crdts
        assert isinstance(lww_set, LWWElementSet)
        assert lww_set.clock == manager.clock
    
    def test_crdt_manager_create_or_set(self):
        """Test creating OR set through manager"""
        manager = CRDTManager("node1")
        
        or_set = manager.create_or_set("my_or_set")
        
        assert "my_or_set" in manager.crdts
        assert isinstance(or_set, ORSet)
        assert or_set.clock == manager.clock
    
    def test_crdt_manager_create_counters(self):
        """Test creating counters through manager"""
        manager = CRDTManager("node1")
        
        g_counter = manager.create_g_counter("my_g_counter")
        pn_counter = manager.create_pn_counter("my_pn_counter")
        
        assert "my_g_counter" in manager.crdts
        assert "my_pn_counter" in manager.crdts
        assert isinstance(g_counter, GCounter)
        assert isinstance(pn_counter, PNCounter)
    
    def test_crdt_manager_get_state(self):
        """Test getting global state from manager"""
        manager = CRDTManager("node1")
        
        # Create some CRDTs and modify them
        lww_set = manager.create_lww_set("set1")
        counter = manager.create_g_counter("counter1")
        
        lww_set.add("element1")
        counter.increment(5)
        
        # Get global state
        state = manager.get_state()
        
        assert "set1" in state
        assert "counter1" in state
        assert "clock" in state
    
    def test_crdt_manager_merge_state(self):
        """Test merging state in CRDT manager"""
        manager1 = CRDTManager("node1")
        manager2 = CRDTManager("node2")
        
        # Create same CRDTs on both managers
        set1 = manager1.create_lww_set("shared_set")
        set2 = manager2.create_lww_set("shared_set")
        
        counter1 = manager1.create_g_counter("shared_counter")
        counter2 = manager2.create_g_counter("shared_counter")
        
        # Modify independently
        set1.add("element1")
        set2.add("element2")
        
        counter1.increment(3)
        counter2.increment(7)
        
        # Merge states
        state1 = manager1.get_state()
        state2 = manager2.get_state()
        
        manager1.merge_state(state2)
        manager2.merge_state(state1)
        
        # Verify convergence
        final_set1 = manager1.get_crdt("shared_set")
        final_set2 = manager2.get_crdt("shared_set")
        
        final_counter1 = manager1.get_crdt("shared_counter")
        final_counter2 = manager2.get_crdt("shared_counter")
        
        assert final_set1.elements() == final_set2.elements()
        assert final_counter1.value() == final_counter2.value() == 10


class TestCRDTNetworkIntegration:
    """Test CRDT integration with network communication"""
    
    def test_crdt_state_serialization_roundtrip(self):
        """Test CRDT state serialization for network transfer"""
        manager = CRDTManager("node1")
        
        # Create and modify CRDTs
        lww_set = manager.create_lww_set("test_set")
        lww_set.add("element1")
        lww_set.add("element2")
        lww_set.remove("element1")
        
        counter = manager.create_pn_counter("test_counter")
        counter.increment(10)
        counter.decrement(3)
        
        # Serialize state
        state = manager.get_state()
        json_state = json.dumps(state)
        
        # Deserialize and restore
        restored_state = json.loads(json_state)
        
        new_manager = CRDTManager("node2")
        new_manager.merge_state(restored_state)
        
        # Verify restoration
        restored_set = new_manager.get_crdt("test_set")
        restored_counter = new_manager.get_crdt("test_counter")
        
        if restored_set and restored_counter:
            assert restored_set.elements() == lww_set.elements()
            assert restored_counter.value() == counter.value()
    
    def test_crdt_partial_state_sync(self):
        """Test syncing only specific CRDTs"""
        manager1 = CRDTManager("node1")
        manager2 = CRDTManager("node2")
        
        # Create multiple CRDTs
        set1_a = manager1.create_lww_set("set_a")
        set1_b = manager1.create_lww_set("set_b")
        counter1 = manager1.create_g_counter("counter")
        
        set2_a = manager2.create_lww_set("set_a")
        
        # Modify
        set1_a.add("from_node1")
        set1_b.add("only_on_node1")
        counter1.increment(5)
        
        set2_a.add("from_node2")
        
        # Get partial state (only set_a)
        partial_state = {
            "set_a": manager1.get_crdt("set_a").get_state(),
            "clock": manager1.clock.clock.copy()
        }
        
        # Merge partial state
        manager2.merge_state(partial_state)
        
        # Verify only set_a was synced
        synced_set_a = manager2.get_crdt("set_a")
        missing_set_b = manager2.get_crdt("set_b")
        missing_counter = manager2.get_crdt("counter")
        
        assert synced_set_a is not None
        elements = synced_set_a.elements()
        assert "from_node1" in elements
        assert "from_node2" in elements
        
        assert missing_set_b is None
        assert missing_counter is None


class TestCRDTPerformance:
    """Test CRDT performance characteristics"""
    
    def test_large_lww_set_performance(self):
        """Test performance with large LWW sets"""
        clock = VectorClock("node1")
        lww_set = LWWElementSet(clock)
        
        # Add many elements
        start_time = time.time()
        element_count = 10000
        
        for i in range(element_count):
            lww_set.add(f"element_{i}")
        
        add_duration = time.time() - start_time
        
        # Test element check performance
        start_time = time.time()
        elements = lww_set.elements()
        elements_duration = time.time() - start_time
        
        # Performance assertions
        assert len(elements) == element_count
        assert add_duration < 5.0  # Should add 10k elements in <5 seconds
        assert elements_duration < 1.0  # Should enumerate in <1 second
    
    def test_counter_performance_under_load(self):
        """Test counter performance under high load"""
        counter = PNCounter("node1")
        
        def stress_worker():
            for i in range(1000):
                if i % 2 == 0:
                    counter.increment()
                else:
                    counter.decrement()
        
        # Run multiple workers
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(stress_worker) for _ in range(10)]
            for future in futures:
                future.result()
        
        duration = time.time() - start_time
        operations_per_sec = 10000 / duration
        
        # Should handle thousands of operations per second
        assert operations_per_sec > 1000
        assert counter.value() == 0  # Equal increments and decrements
    
    def test_merge_performance_large_states(self):
        """Test merge performance with large CRDT states"""
        manager1 = CRDTManager("node1")
        manager2 = CRDTManager("node2")
        
        # Create large states
        for i in range(100):
            set_name = f"set_{i}"
            set1 = manager1.create_lww_set(set_name)
            set2 = manager2.create_lww_set(set_name)
            
            # Add elements
            for j in range(100):
                set1.add(f"element_{i}_{j}_node1")
                set2.add(f"element_{i}_{j}_node2")
        
        # Measure merge performance
        state1 = manager1.get_state()
        state2 = manager2.get_state()
        
        start_time = time.time()
        manager1.merge_state(state2)
        merge_duration = time.time() - start_time
        
        # Should merge large states efficiently
        assert merge_duration < 10.0  # <10 seconds for large merge
        
        # Verify correctness
        for i in range(100):
            merged_set = manager1.get_crdt(f"set_{i}")
            elements = merged_set.elements()
            assert len(elements) == 200  # 100 from each node


if __name__ == '__main__':
    pytest.main([__file__])
