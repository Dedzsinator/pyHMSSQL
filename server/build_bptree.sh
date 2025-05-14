#!/bin/bash
# Build script for optimized B+ tree implementation

# Clean any previous build artifacts
echo "Cleaning previous build..."
rm -f bptree_optimized*.so
rm -rf build/

echo "Building optimized B+ tree implementation..."
CFLAGS="-O3" python setup.py build_ext --inplace || {
    echo "Error: Failed to build bptree_optimized extension"
    exit 1
}

echo "Testing optimized implementation..."
python -c "import bptree_optimized; print('Successfully imported optimized implementation')" || {
    echo "Error: Failed to import bptree_optimized"
    exit 1
}

# Create or update the adapter module
if [ ! -f bptree_adapter.py ]; then
    echo "Creating adapter module..."
    cp bptree_wrapper.py bptree_adapter.py
fi

echo "Done! Successfully built optimized B+ tree implementation."
echo "Use bptree_adapter.py to choose between implementations."