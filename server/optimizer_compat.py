"""
Optimizer Compatibility Layer.

This module provides backward compatibility with the existing optimizer interface
while leveraging the new unified optimizer architecture internally.
"""

import logging
import warnings
from typing import Dict, Any, Optional

# Legacy imports for backward compatibility
from bptree_adapter import BPlusTree
from table_stats import TableStatistics

# Import the new unified optimizer
try:
    from .unified_optimizer import UnifiedQueryOptimizer, OptimizationLevel, OptimizationOptions
    UNIFIED_AVAILABLE = True
except ImportError:
    UNIFIED_AVAILABLE = False
    logging.warning("Unified optimizer not available - disabling advanced features")


class LegacyOptimizer:
    """
    Legacy optimizer interface for backward compatibility.
    
    This class wraps the new unified optimizer to provide the same interface
    as the original optimizer while leveraging the advanced features internally.
    """
    
    def __init__(self, catalog_manager, index_manager):
        """Initialize the legacy optimizer."""
        self.catalog_manager = catalog_manager
        self.index_manager = index_manager
        
        # Initialize statistics components for compatibility
        self.statistics = TableStatistics(catalog_manager)
        
        # Initialize unified optimizer if available
        self.unified_optimizer = None
        if UNIFIED_AVAILABLE:
            try:
                options = OptimizationOptions.for_level(OptimizationLevel.STANDARD)
                self.unified_optimizer = UnifiedQueryOptimizer(
                    catalog_manager, index_manager, options
                )
            except Exception as e:
                logging.warning(f"Failed to initialize unified optimizer: {e}")
                self.unified_optimizer = None
        
        # Legacy compatibility attributes
        self.optimization_time = 0
        self.optimization_count = 0
        self.plan_cache = {}
        
        logging.info("Legacy optimizer initialized with unified backend" if self.unified_optimizer 
                    else "Legacy optimizer initialized in compatibility mode")
    
    def optimize(self, plan, plan_type=None):
        """
        Optimize the execution plan using the unified optimizer if available.
        
        Args:
            plan: The execution plan to optimize
            plan_type: Optional plan type override
            
        Returns:
            The optimized execution plan
        """
        if not plan or not isinstance(plan, dict):
            return plan
        
        # Use unified optimizer if available
        if self.unified_optimizer:
            try:
                # Generate a query signature for caching
                query_signature = self._generate_query_signature(plan)
                
                result = self.unified_optimizer.optimize(plan, query_signature)
                
                # Update legacy statistics
                self.optimization_time += result.optimization_time_ms / 1000.0
                self.optimization_count += 1
                
                return result.optimized_plan
                
            except Exception as e:
                logging.error(f"Unified optimization failed, falling back: {e}")
                # Fall through to legacy behavior
        
        # Legacy fallback - just return the original plan
        warnings.warn("Using legacy optimizer fallback - limited optimization capabilities", 
                     DeprecationWarning)
        
        self.optimization_count += 1
        return plan
    
    def _generate_query_signature(self, plan: Dict[str, Any]) -> str:
        """Generate a signature for query caching."""
        # Create a simple signature based on plan structure
        signature_parts = []
        
        if 'type' in plan:
            signature_parts.append(f"type:{plan['type']}")
        
        if 'table' in plan:
            signature_parts.append(f"table:{plan['table']}")
        
        if 'condition' in plan:
            # Normalize condition for better caching
            condition = str(plan['condition']).lower().strip()
            signature_parts.append(f"condition:{condition}")
        
        return "|".join(signature_parts)
    
    def refresh_statistics(self, table_name=None):
        """Refresh statistics for optimization."""
        if self.unified_optimizer:
            self.unified_optimizer.refresh_statistics(table_name)
        else:
            # Legacy statistics refresh
            if table_name:
                self.statistics.collect_table_statistics(table_name)
            else:
                # This is a simplified version
                pass
    
    def get_optimization_stats(self):
        """Get optimization statistics."""
        if self.unified_optimizer:
            stats = self.unified_optimizer.get_optimization_statistics()
            # Map to legacy format
            return {
                "total_optimizations": stats.get('total_optimizations', self.optimization_count),
                "total_time_ms": stats.get('total_time_ms', self.optimization_time * 1000),
                "avg_time_ms": stats.get('avg_optimization_time_ms', 0),
                "cache_stats": stats.get('cache_statistics', {})
            }
        else:
            # Legacy statistics
            avg_time = 0
            if self.optimization_count > 0:
                avg_time = self.optimization_time / self.optimization_count
            
            return {
                "total_optimizations": self.optimization_count,
                "total_time_ms": self.optimization_time * 1000,
                "avg_time_ms": avg_time * 1000,
                "cache_stats": {}
            }
    
    def shutdown(self):
        """Shutdown the optimizer."""
        if self.unified_optimizer:
            self.unified_optimizer.shutdown()


# For backward compatibility, create an alias
Optimizer = LegacyOptimizer
