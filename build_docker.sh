#!/bin/bash

# Build script for pyHMSSQL with optimized B+ tree
set -e

echo "🚀 Building pyHMSSQL Docker image with optimized B+ tree..."

# Set build arguments
BUILD_VERSION=${1:-"latest"}
DOCKERFILE=${2:-"Dockerfile"}

echo "📋 Build Configuration:"
echo "  - Version: $BUILD_VERSION"
echo "  - Dockerfile: $DOCKERFILE"
echo "  - Date: $(date)"

# Build the Docker image
echo "🔨 Building Docker image..."
docker build \
    --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --build-arg VERSION="$BUILD_VERSION" \
    -t pyhmssql:$BUILD_VERSION \
    -f $DOCKERFILE \
    .

echo "✅ Docker image built successfully!"

# Test the image
echo "🧪 Testing the built image..."
echo "Starting container to verify B+ tree compilation..."

# Run a quick test to verify the B+ tree is properly compiled
docker run --rm pyhmssql:$BUILD_VERSION python -c "
import sys
print('Testing optimized B+ tree import...')
try:
    from bptree import BPTreeOptimized
    print('✅ Optimized B+ tree import successful')
    
    # Test basic functionality
    tree = BPTreeOptimized()
    print('✅ B+ tree instantiation successful')
    
    print('🎉 All tests passed!')
except Exception as e:
    print(f'❌ Test failed: {e}')
    sys.exit(1)
"

if [ $? -eq 0 ]; then
    echo "✅ Image verification successful!"
    echo "🎉 pyHMSSQL:$BUILD_VERSION is ready for use!"
    echo ""
    echo "To run the container:"
    echo "  docker run -p 9999:9999 -p 5000:5000 pyhmssql:$BUILD_VERSION"
    echo ""
    echo "To run with volume mounts:"
    echo "  docker run -p 9999:9999 -p 5000:5000 -v \$(pwd)/data:/app/data pyhmssql:$BUILD_VERSION"
else
    echo "❌ Image verification failed!"
    exit 1
fi
