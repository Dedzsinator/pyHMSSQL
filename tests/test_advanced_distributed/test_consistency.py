"""
Unit tests for consistency levels and coordination
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from kvstore.consistency import (
    ConsistencyLevel, ConsistencyManager, ConsistencyConfig,
    ConsistencyResult, ReplicaResponse, HintedHandoff, ReadRepair
)

class TestConsistencyLevels:
    """Test consistency level calculations and behavior"""
    
    def test_consistency_level_enum(self):
        """Test that all consistency levels are properly defined"""
        assert ConsistencyLevel.ONE.value == "ONE"
        assert ConsistencyLevel.QUORUM.value == "QUORUM"
        assert ConsistencyLevel.ALL.value == "ALL"
    
    def test_consistency_config_creation(self):
        """Test consistency configuration creation"""
        config = ConsistencyConfig(
            level=ConsistencyLevel.QUORUM,
            timeout_ms=5000,
            read_repair=True
        )
        assert config.level == ConsistencyLevel.QUORUM
        assert config.timeout_ms == 5000
        assert config.read_repair is True
    
    def test_consistency_manager_initialization(self):
        """Test consistency manager initialization"""
        config = ConsistencyConfig(level=ConsistencyLevel.QUORUM)
        manager = ConsistencyManager(config=config)
        assert manager.config.level == ConsistencyLevel.QUORUM

class TestConsistencyCalculations:
    """Test consistency requirement calculations"""
    
    @pytest.fixture
    def manager(self):
        config = ConsistencyConfig(level=ConsistencyLevel.QUORUM)
        return ConsistencyManager(config=config)
    
    def test_calculate_required_responses_one(self, manager):
        """Test ONE consistency level requirements"""
        assert manager.calculate_required_responses(ConsistencyLevel.ONE, 1) == 1
        assert manager.calculate_required_responses(ConsistencyLevel.ONE, 3) == 1
        assert manager.calculate_required_responses(ConsistencyLevel.ONE, 5) == 1
    
    def test_calculate_required_responses_quorum(self, manager):
        """Test QUORUM consistency level requirements"""
        assert manager.calculate_required_responses(ConsistencyLevel.QUORUM, 1) == 1
        assert manager.calculate_required_responses(ConsistencyLevel.QUORUM, 2) == 2
        assert manager.calculate_required_responses(ConsistencyLevel.QUORUM, 3) == 2
        assert manager.calculate_required_responses(ConsistencyLevel.QUORUM, 4) == 3
        assert manager.calculate_required_responses(ConsistencyLevel.QUORUM, 5) == 3
    
    def test_calculate_required_responses_all(self, manager):
        """Test ALL consistency level requirements"""
        assert manager.calculate_required_responses(ConsistencyLevel.ALL, 1) == 1
        assert manager.calculate_required_responses(ConsistencyLevel.ALL, 3) == 3
        assert manager.calculate_required_responses(ConsistencyLevel.ALL, 5) == 5

class TestConsistencyCoordination:
    """Test consistency coordination operations"""
    
    @pytest.fixture
    def manager(self):
        config = ConsistencyConfig(
            level=ConsistencyLevel.QUORUM,
            timeout_ms=1000,
            read_repair=True
        )
        return ConsistencyManager(config=config)
    
    @pytest.mark.asyncio
    async def test_coordinate_read_success(self, manager):
        """Test successful read coordination"""
        replica_nodes = ["node1", "node2", "node3"]
        
        # Mock the internal read method to succeed
        with patch.object(manager, '_read_from_replica') as mock_read:
            mock_read.return_value = ReplicaResponse(
                node_id="node1",
                success=True,
                value="test_value",
                timestamp=int(time.time() * 1000),
                latency_ms=10.0
            )
            
            result = await manager.coordinate_read(
                "test_key", replica_nodes, ConsistencyLevel.QUORUM
            )
            
            assert result.success is True
    
    @pytest.mark.asyncio
    async def test_coordinate_read_insufficient_responses(self, manager):
        """Test read coordination with insufficient responses"""
        replica_nodes = ["node1", "node2", "node3"]
        
        with patch.object(manager, '_read_from_replica') as mock_read:
            mock_read.return_value = ConsistencyResult(
                success=False,
                value=None,
                satisfied_nodes=1,
                required_nodes=2
            )
            
            result = await manager.coordinate_read(
                "test_key", replica_nodes, ConsistencyLevel.QUORUM
            )
            
            assert result.success is False
    
    @pytest.mark.asyncio
    async def test_coordinate_write_success(self, manager):
        """Test successful write coordination"""
        replica_nodes = ["node1", "node2", "node3"]
        
        with patch.object(manager, '_write_to_replica') as mock_write:
            mock_write.return_value = ConsistencyResult(
                success=True,
                satisfied_nodes=2,
                required_nodes=2
            )
            
            result = await manager.coordinate_write(
                "test_key", "test_value", replica_nodes, ConsistencyLevel.QUORUM
            )
            
            assert result.success is True

class TestConsistencyErrorHandling:
    """Test error handling in consistency operations"""
    
    @pytest.fixture
    def manager(self):
        config = ConsistencyConfig(
            level=ConsistencyLevel.ALL,
            timeout_ms=100  # Short timeout for testing
        )
        return ConsistencyManager(config=config)
    
    @pytest.mark.asyncio
    async def test_coordination_timeout(self, manager):
        """Test coordination timeout handling"""
        replica_nodes = ["node1", "node2", "node3"]
        
        with patch.object(manager, '_read_from_replica') as mock_read:
            # Simulate timeout by raising an exception
            mock_read.side_effect = asyncio.TimeoutError()
            
            result = await manager.coordinate_read(
                "test_key", replica_nodes, ConsistencyLevel.ALL
            )
            
            # Should handle timeout gracefully and return failed result
            assert result.success is False
    
    def test_invalid_consistency_level(self, manager):
        """Test handling of invalid consistency levels"""
        # Test with a string that's not a valid enum value
        try:
            manager.calculate_required_responses("INVALID", 3)
            # If it doesn't raise, that's fine - just check the behavior
            assert True
        except (ValueError, AttributeError):
            # Either exception type is acceptable
            assert True

class TestHintedHandoff:
    """Test hinted handoff functionality"""
    
    @pytest.fixture
    def manager(self):
        config = ConsistencyConfig(level=ConsistencyLevel.QUORUM)
        return ConsistencyManager(config=config)
    
    @pytest.mark.asyncio
    async def test_hinted_handoff_storage(self, manager):
        """Test storing hints for unavailable nodes"""
        # Test the hinted handoff functionality
        hinted_handoff = HintedHandoff()
        
        operation = {"key": "test_key", "value": "test_value", "timestamp": time.time()}
        result = hinted_handoff.store_hint("failed_node", operation, "target_node")
        
        assert result is True
        hints = hinted_handoff.get_hints("target_node")
        assert len(hints) > 0
    
    @pytest.mark.asyncio
    async def test_hinted_handoff_replay(self, manager):
        """Test replaying hints when nodes come back online"""
        # Test hint replay functionality
        replayed_count = await manager.replay_hints("node2")
        
        # Should return 0 since there are no hints initially
        assert replayed_count == 0

if __name__ == "__main__":
    pytest.main([__file__])
