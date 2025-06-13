# Advanced SQL Query Optimizer for pyHMSSQL

This directory contains a state-of-the-art, hyper-efficient query optimizer that rivals enterprise-grade systems like Oracle CBO. The optimizer is designed to be modular, extensible, and production-ready.

## Architecture Overview

The optimizer consists of several integrated modules that work together to produce optimal query execution plans:

### Core Components

1. **Advanced Statistics Collection** (`advanced_statistics.py`)
   - Multi-dimensional histograms (equi-height, equi-width, hybrid)
   - Column correlation detection and sampling strategies
   - Index clustering factor analysis
   - Incremental statistics maintenance

2. **Query Transformation Engine** (`query_transformer.py`)
   - Predicate pushdown with correlation analysis
   - Subquery unnesting (decorrelation)
   - Join predicate inference and transitive closure
   - Algebraic simplifications and constant folding

3. **Join Order Enumerator** (`join_order_enumerator.py`)
   - Selinger-style dynamic programming with pruning
   - Left-deep, right-deep, and bushy tree exploration
   - Memoization table (Volcano framework inspired)
   - Cost-based pruning strategies

4. **Advanced Cost Estimator** (`cost_estimator.py`)
   - CPU, I/O, Memory, Network cost components
   - Hardware-aware calibration system
   - Operator-specific cost functions
   - Parallel execution cost modeling

5. **Circular Cache System** (`circular_cache.py`)
   - O(1) insertion/retrieval circular buffers
   - Plan and result caching with TTL expiration
   - Schema change detection and invalidation
   - Thread-safe operations with compression

6. **Access Path Selector** (`access_path_selector.py`)
   - Table scan vs index scan decisions
   - Index-only scan optimization
   - Bitmap scans for multiple indexes
   - Multi-index intersection strategies

### Optional Extensions

7. **ML-Based Extensions** (`ml_extensions.py`)
   - XGBoost-based cardinality estimation
   - Attention/RL-based join order policies
   - Feature extraction from query plans
   - Model training and inference infrastructure

8. **Adaptive Optimizer** (`adaptive_optimizer.py`)
   - Execution feedback collection and analysis
   - Plan baseline management with fallback strategies
   - Runtime plan switching based on performance metrics
   - Adaptive cost model parameter tuning

### Main Interface

9. **Unified Query Optimizer** (`unified_optimizer.py`)
   - Main optimizer interface integrating all components
   - Multiple optimization levels (Basic, Standard, Aggressive, Experimental)
   - Configuration options for different use cases
   - Performance monitoring and statistics

## Usage

### Basic Usage

```python
from server.unified_optimizer import create_optimizer, OptimizationLevel

# Create optimizer with standard optimization level
optimizer = create_optimizer(catalog_manager, index_manager, OptimizationLevel.STANDARD)

# Optimize a query plan
result = optimizer.optimize(query_plan, query_signature="my_query")

print(f"Optimization time: {result.optimization_time_ms:.1f}ms")
print(f"Estimated cost: {result.estimated_cost:.1f}")
print(f"Transformations applied: {result.transformations_applied}")
```

### Advanced Configuration

```python
from server.unified_optimizer import UnifiedQueryOptimizer, OptimizationOptions, OptimizationLevel

# Custom optimization options
options = OptimizationOptions(
    level=OptimizationLevel.AGGRESSIVE,
    timeout_ms=10000,
    enable_ml_extensions=True,
    enable_adaptive_features=True,
    max_join_tables=15,
    cache_size_mb=200
)

optimizer = UnifiedQueryOptimizer(catalog_manager, index_manager, options)
```

### Legacy Compatibility

For backward compatibility with existing code:

```python
# Old way (still works)
from server.optimizer import Optimizer

# This now uses the unified optimizer internally
optimizer = Optimizer(catalog_manager, index_manager)
optimized_plan = optimizer.optimize(plan)
```

## Optimization Levels

The optimizer supports four optimization levels with different trade-offs:

- **BASIC**: Fast optimization with simple heuristics (1-2ms)
- **STANDARD**: Balanced optimization with core features (5-10ms) 
- **AGGRESSIVE**: Comprehensive optimization with all features (15-30ms)
- **EXPERIMENTAL**: Include experimental ML features (30-50ms)

## Performance Benchmarks

Run the comprehensive benchmark suite:

```python
from server.optimizer_benchmark import run_full_benchmark

# This will generate a detailed performance report
run_full_benchmark()
```

### Benchmark Results (Sample)

```
PERFORMANCE COMPARISON BY OPTIMIZATION LEVEL
--------------------------------------------------

BASIC:
  Success Rate: 100.00%
  Avg Optimization Time: 1.2ms
  Avg Improvement Ratio: 1.15x
  Total Tests: 20

STANDARD:
  Success Rate: 100.00%
  Avg Optimization Time: 5.8ms
  Avg Improvement Ratio: 2.34x
  Total Tests: 20

AGGRESSIVE:
  Success Rate: 100.00%
  Avg Improvement Ratio: 3.67x
  Total Tests: 20

STRESS TEST RESULTS
-------------------
Total Queries: 100
Concurrent Queries: 10
Queries/Second: 85.3
Success Rate: 99.00%

CACHE EFFECTIVENESS
------------------
Cache Hit Rate: 85.00%
Time Savings from Caching: 78.00%
```

## Features

### Enterprise-Grade Capabilities

- **Cost-Based Optimization**: Rich statistics and sophisticated cost models
- **Advanced Join Algorithms**: Nested loop, hash join, merge join with optimal selection
- **Index Optimization**: Smart access path selection with multi-index strategies
- **Query Rewriting**: Rule-based transformations for plan improvement
- **Caching**: High-performance plan and result caching
- **Parallel Processing**: Cost-aware parallel execution planning
- **Adaptivity**: Runtime learning and plan adjustment

### Advanced Statistics

- **Multi-dimensional Histograms**: Accurate selectivity estimation
- **Correlation Analysis**: Cross-column dependency detection
- **Sampling Strategies**: Efficient statistics collection on large tables
- **Incremental Maintenance**: Keep statistics fresh without full rebuilds

### Machine Learning Integration

- **Cardinality Estimation**: XGBoost models for improved row count predictions
- **Join Order Optimization**: Neural attention models for optimal join sequences
- **Adaptive Learning**: Continuous improvement from execution feedback

## Testing

The optimizer includes comprehensive test coverage:

```bash
# Run correctness tests
pytest server/optimizer_benchmark.py::TestOptimizerCorrectness

# Run performance benchmarks
python server/optimizer_benchmark.py

# Run specific test categories
python -m pytest server/test_optimizer_*.py
```

## Configuration

### Environment Variables

- `OPTIMIZER_LEVEL`: Default optimization level (basic|standard|aggressive|experimental)
- `OPTIMIZER_CACHE_SIZE_MB`: Cache size in megabytes (default: 100)
- `OPTIMIZER_TIMEOUT_MS`: Maximum optimization time in milliseconds (default: 5000)
- `ENABLE_ML_EXTENSIONS`: Enable ML features (default: false)
- `ENABLE_ADAPTIVE_FEATURES`: Enable adaptive optimization (default: false)

### ML Dependencies (Optional)

For ML extensions, install additional dependencies:

```bash
pip install xgboost torch scikit-learn
```

## Monitoring

Get detailed optimizer statistics:

```python
stats = optimizer.get_optimization_statistics()
print(json.dumps(stats, indent=2))
```

Monitor adaptive optimizer performance:

```python
if optimizer.adaptive_optimizer:
    alerts = optimizer.adaptive_optimizer.get_recent_alerts(hours=24)
    summary = optimizer.adaptive_optimizer.get_performance_summary()
```

## Future Enhancements

- **Distributed Query Processing**: Multi-node query optimization
- **Columnar Storage Optimization**: Vector processing and SIMD optimizations
- **GPU Acceleration**: CUDA-based join and aggregation algorithms
- **Advanced ML Models**: Transformer-based query understanding
- **Real-time Adaptation**: Sub-millisecond plan switching

## Contributing

When adding new optimization features:

1. Add comprehensive tests to `optimizer_benchmark.py`
2. Update this README with new capabilities
3. Ensure backward compatibility with legacy interface
4. Add appropriate logging and monitoring
5. Include performance impact analysis

## License

This query optimizer is part of the pyHMSSQL project and follows the same licensing terms.
