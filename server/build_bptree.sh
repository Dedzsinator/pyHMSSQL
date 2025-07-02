#!/bin/bash
# Build script for optimized B+ tree implementation

set -e  # Exit on any error

echo "======================================="
echo "Building Optimized B+ Tree for pyHMSSQL"
echo "======================================="

# Clean any previous build artifacts
echo "Cleaning previous build..."
rm -f bptree*.so bptree*.c
rm -rf build/ __pycache__/

# Check for required dependencies
echo "Checking dependencies..."
python3 -c "import numpy; print(f'NumPy version: {numpy.__version__}')" || {
    echo "Error: NumPy not found. Installing..."
    pip install numpy
}

python3 -c "import cython; print(f'Cython version: {cython.__version__}')" || {
    echo "Error: Cython not found. Installing..."
    pip install cython
}

echo "Building optimized B+ tree implementation..."
CFLAGS="-O3 -march=native -ffast-math" python3 setup.py build_ext --inplace || {
    echo "Error: Failed to build bptree extension"
    exit 1
}

echo "Testing optimized implementation..."
python3 -c "
try:
    from bptree import BPlusTreeOptimized
    tree = BPlusTreeOptimized(order=10, name='test')
    tree.insert(1, 'test_value')
    result = tree.search(1)
    assert result == 'test_value', f'Expected test_value, got {result}'
    print('✓ Basic functionality test passed')
    
    # Test multidimensional support
    tree.insert_multidim([1.0, 2.0], 'multi_test')
    multi_result = tree.search_multidim([1.0, 2.0])
    assert multi_result == 'multi_test', f'Expected multi_test, got {multi_result}'
    print('✓ Multidimensional functionality test passed')
    
    print('✓ Successfully imported and tested optimized implementation')
except Exception as e:
    print(f'✗ Error testing implementation: {e}')
    exit(1)
" || {
    echo "Error: Failed to import or test bptree_optimized"
    exit 1
}

# Create or update the adapter module
if [ ! -f bptree_adapter.py ]; then
    echo "Creating adapter module..."
    cp bptree_wrapper.py bptree_adapter.py 2>/dev/null || echo "Warning: bptree_wrapper.py not found"
fi

echo "======================================="
echo "✓ Build completed successfully!"
echo "======================================="
echo "Files created:"
ls -la bptree*.so 2>/dev/null || echo "  No .so files found"
echo "Use bptree_adapter.py or directly import bptree"