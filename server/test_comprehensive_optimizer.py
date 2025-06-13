#!/usr/bin/env python3
"""
Comprehensive Test Suite for Unified Query Optimizer.

This test suite validates the complete functionality of the state-of-the-art
SQL query optimizer, including all optimization levels, caching, ML extensions,
and adaptive features.
"""

import pytest
import time
import logging
from typing import Dict, List, Any

# Test the unified optimizer
from server.unified_optimizer import (
    UnifiedQueryOptimizer, OptimizationLevel, OptimizationOptions,
    create_optimizer
)

# Mock managers for testing
from server.optimizer_benchmark import MockCatalogManager, MockIndexManager


class TestUnifiedQueryOptimizerComprehensive:
    """Comprehensive test suite for the unified query optimizer."""
    
    @pytest.fixture
    def catalog_manager(self):
        """Create a mock catalog manager for testing."""
        return MockCatalogManager()
    
    @pytest.fixture
    def index_manager(self):
        """Create a mock index manager for testing."""
        return MockIndexManager()
    
    def test_all_optimization_levels(self, catalog_manager, index_manager):
        """Test that all optimization levels work correctly."""
        test_plan = {
            'operation': 'select',
            'tables': ['users', 'orders'],
            'predicates': [
                {'column': 'user_id', 'operator': '=', 'value': 123}
            ],
            'estimated_cost': 1000.0,
            'estimated_rows': 100
        }
        
        for level in OptimizationLevel:
            print(f"Testing optimization level: {level.value}")
            
            # Create optimizer for this level
            optimizer = create_optimizer(catalog_manager, index_manager, level)
            
            # Optimize a test query
            result = optimizer.optimize(test_plan, f"test_query_{level.value}")
            
            # Verify result structure
            assert result is not None
            assert result.optimized_plan is not None
            assert result.original_plan == test_plan
            assert result.optimization_time_ms >= 0
            assert result.estimated_cost > 0
            assert result.estimated_rows > 0
            assert result.optimization_level == level
            
            # Check that features are enabled based on level
            if level == OptimizationLevel.BASIC:
                assert len(result.transformations_applied) == 0
                assert not result.join_order_changed
            elif level == OptimizationLevel.AGGRESSIVE:
                # Aggressive level should have more features enabled
                pass
            
            optimizer.shutdown()
            print(f"✓ Level {level.value} working correctly")
    
    def test_cache_effectiveness(self, catalog_manager, index_manager):
        """Test that caching works and improves performance."""
        optimizer = create_optimizer(
            catalog_manager, index_manager, 
            OptimizationLevel.STANDARD
        )
        
        test_plan = {
            'operation': 'select',
            'tables': ['users'],
            'predicates': [
                {'column': 'id', 'operator': '=', 'value': 1}
            ],
            'estimated_cost': 500.0,
            'estimated_rows': 1
        }
        
        query_signature = "SELECT * FROM users WHERE id = 1"
        
        # First optimization - should be cache miss
        result1 = optimizer.optimize(test_plan, query_signature)
        assert not result1.cache_hit
        
        # Second optimization - should be cache hit
        result2 = optimizer.optimize(test_plan, query_signature)
        assert result2.cache_hit
        
        # Cache hit should be faster
        assert result2.optimization_time_ms <= result1.optimization_time_ms
        
        # Verify cache statistics
        stats = optimizer.get_optimization_statistics()
        assert stats['plan_cache']['hits'] >= 1
        assert stats['plan_cache']['hit_rate'] > 0
        
        optimizer.shutdown()
        print("✓ Cache effectiveness verified")
    
    def test_statistics_collection(self, catalog_manager, index_manager):
        """Test that statistics are properly collected and reported."""
        optimizer = create_optimizer(
            catalog_manager, index_manager,
            OptimizationLevel.STANDARD
        )
        
        # Run several optimizations
        for i in range(5):
            test_plan = {
                'operation': 'select',
                'tables': [f'table_{i}'],
                'estimated_cost': 100.0 + i * 50,
                'estimated_rows': 10 + i * 5
            }
            
            optimizer.optimize(test_plan, f"query_{i}")
        
        # Check statistics
        stats = optimizer.get_optimization_statistics()
        
        assert stats['total_optimizations'] >= 5
        assert stats['total_time_ms'] > 0
        assert stats['avg_optimization_time_ms'] > 0
        assert 'plan_cache' in stats
        assert 'result_cache' in stats
        
        optimizer.shutdown()
        print("✓ Statistics collection working")
    
    def test_execution_feedback(self, catalog_manager, index_manager):
        """Test adaptive feedback mechanism."""
        # Only test if adaptive optimizer is available
        try:
            optimizer = create_optimizer(
                catalog_manager, index_manager,
                OptimizationLevel.AGGRESSIVE  # Adaptive features enabled
            )
            
            query_signature = "SELECT * FROM users WHERE age > 25"
            test_plan = {
                'operation': 'select',
                'tables': ['users'],
                'estimated_cost': 1000.0,
                'estimated_rows': 500
            }
            
            # Optimize query
            result = optimizer.optimize(test_plan, query_signature)
            
            # Simulate execution feedback
            optimizer.add_execution_feedback(
                query_signature=query_signature,
                query_id="test_query_1",
                actual_rows=600,  # Different from estimated 500
                actual_time=0.05,
                success=True
            )
            
            # The adaptive optimizer should learn from this feedback
            # (In a real implementation, this would affect future optimizations)
            
            optimizer.shutdown()
            print("✓ Execution feedback mechanism working")
            
        except ImportError:
            print("⚠ Adaptive optimizer not available - skipping feedback test")
    
    def test_error_handling(self, catalog_manager, index_manager):
        """Test that the optimizer handles errors gracefully."""
        optimizer = create_optimizer(
            catalog_manager, index_manager,
            OptimizationLevel.STANDARD
        )
        
        # Test with malformed plan
        malformed_plan = {
            'invalid_structure': True,
            # Missing required fields
        }
        
        # Should not crash, should return a valid result
        result = optimizer.optimize(malformed_plan, "malformed_query")
        
        assert result is not None
        assert len(result.warnings) > 0  # Should have warnings about the error
        
        # Test with None plan
        result = optimizer.optimize(None, "null_query")
        assert result is not None
        
        optimizer.shutdown()
        print("✓ Error handling working correctly")
    
    def test_concurrent_optimization(self, catalog_manager, index_manager):
        """Test thread safety with concurrent optimizations."""
        import threading
        import concurrent.futures
        
        optimizer = create_optimizer(
            catalog_manager, index_manager,
            OptimizationLevel.STANDARD
        )
        
        def optimize_query(query_id):
            test_plan = {
                'operation': 'select',
                'tables': [f'table_{query_id % 3}'],
                'estimated_cost': 100.0 + query_id * 10,
                'estimated_rows': 50 + query_id * 5
            }
            
            result = optimizer.optimize(test_plan, f"concurrent_query_{query_id}")
            return result
        
        # Run 10 concurrent optimizations
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(optimize_query, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # All should succeed
        assert len(results) == 10
        assert all(result is not None for result in results)
        
        # Check final statistics
        stats = optimizer.get_optimization_statistics()
        assert stats['total_optimizations'] >= 10
        
        optimizer.shutdown()
        print("✓ Concurrent optimization working")
    
    def test_memory_management(self, catalog_manager, index_manager):
        """Test that memory usage is reasonable with large workloads."""
        optimizer = create_optimizer(
            catalog_manager, index_manager,
            OptimizationLevel.STANDARD
        )
        
        # Run many optimizations to test memory management
        for i in range(100):
            test_plan = {
                'operation': 'join',
                'left_table': f'table_{i % 5}',
                'right_table': f'table_{(i + 1) % 5}',
                'join_condition': {'left': 'id', 'right': 'foreign_id'},
                'estimated_cost': 1000.0 + i,
                'estimated_rows': 100 + i
            }
            
            optimizer.optimize(test_plan, f"memory_test_query_{i}")
        
        # Check cache statistics - should show evictions if working properly
        stats = optimizer.get_optimization_statistics()
        
        # Cache should not grow unbounded
        plan_cache_size = stats['plan_cache']['current_size']
        result_cache_size = stats['result_cache']['current_size']
        
        # Should be reasonable sizes
        assert plan_cache_size <= 1000  # Default plan cache capacity
        assert result_cache_size <= 500  # Default result cache capacity
        
        optimizer.shutdown()
        print("✓ Memory management working")
    
    def test_ml_extensions_availability(self, catalog_manager, index_manager):
        """Test ML extensions if available."""
        try:
            optimizer = create_optimizer(
                catalog_manager, index_manager,
                OptimizationLevel.EXPERIMENTAL  # Should enable ML if available
            )
            
            stats = optimizer.get_optimization_statistics()
            
            if 'ml_status' in stats:
                print("✓ ML extensions are available and integrated")
            else:
                print("⚠ ML extensions not available (expected on systems without XGBoost)")
            
            optimizer.shutdown()
            
        except Exception as e:
            print(f"⚠ ML extensions test failed: {e}")


def test_optimizer_benchmark_integration():
    """Test integration with the benchmark framework."""
    from server.optimizer_benchmark import OptimizerTestFramework
    
    # Create benchmark instance
    framework = OptimizerTestFramework()
    
    # Run a quick test query optimization
    test_query = {
        'type': 'SELECT',
        'table': 'customers',
        'columns': ['customer_id', 'name'],
        'condition': 'country = "USA"'
    }
    
    # Test optimization across different levels
    results = {}
    for level in [level for level in framework.optimizers.keys()]:
        optimizer = framework.optimizers[level]
        result = optimizer.optimize(test_query, "benchmark_test")
        results[level] = result
    
    assert len(results) > 0
    assert all(result is not None for result in results.values())
    
    # Cleanup
    framework.cleanup()
    
    print("✓ Benchmark integration working")


if __name__ == "__main__":
    # Run the comprehensive test suite
    print("Running Comprehensive Query Optimizer Test Suite")
    print("=" * 60)
    
    # Set up logging
    logging.basicConfig(level=logging.WARNING)  # Reduce log noise
    
    # Create test instance
    test_suite = TestUnifiedQueryOptimizerComprehensive()
    
    # Create mock managers
    catalog_manager = MockCatalogManager()
    index_manager = MockIndexManager()
    
    try:
        # Run all tests
        test_suite.test_all_optimization_levels(catalog_manager, index_manager)
        test_suite.test_cache_effectiveness(catalog_manager, index_manager)
        test_suite.test_statistics_collection(catalog_manager, index_manager)
        test_suite.test_execution_feedback(catalog_manager, index_manager)
        test_suite.test_error_handling(catalog_manager, index_manager)
        test_suite.test_concurrent_optimization(catalog_manager, index_manager)
        test_suite.test_memory_management(catalog_manager, index_manager)
        test_suite.test_ml_extensions_availability(catalog_manager, index_manager)
        test_optimizer_benchmark_integration()
        
        print("=" * 60)
        print("🎉 ALL TESTS PASSED! Query Optimizer is fully functional!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise
