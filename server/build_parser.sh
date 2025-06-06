#!/bin/bash
# filepath: /home/deginandor/Documents/Programming/pyHMSSQL/server/build_parser.sh

set -e

echo "Setting up SQLGlot SQL Parser for pyHMSSQL..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"

# Check if pip is available
if ! command -v pip &> /dev/null; then
    echo "❌ Error: pip is not installed."
    echo "Please install pip first."
    exit 1
fi

# Install SQLGlot and dependencies
echo "📦 Installing SQLGlot and dependencies..."
pip install sqlglot

# Install test dependencies
echo "📦 Installing test dependencies..."
pip install pytest pytest-cov pytest-benchmark pytest-xdist

# Verify installation
echo "🔍 Verifying SQLGlot installation..."
python3 -c "import sqlglot; print(f'✅ SQLGlot {sqlglot.__version__} installed successfully')" || {
    echo "❌ SQLGlot installation verification failed"
    exit 1
}

# Test the parser
echo "🧪 Testing SQLGlot parser..."
python3 -c "
from sqlglot_parser import SQLGlotParser
parser = SQLGlotParser()
result = parser.parse('SELECT * FROM users WHERE id = 1')
if 'type' in result:
    print('✅ SQLGlot parser test passed')
else:
    print('❌ SQLGlot parser test failed')
    print(result)
" || {
    echo "❌ SQLGlot parser test failed"
    exit 1
}

echo ""
echo "✅ SQLGlot SQL parser setup complete!"
echo ""
echo "📝 SQLGlot provides:"
echo "  - Robust SQL parsing for multiple dialects"
echo "  - Query optimization capabilities"
echo "  - SQL transpilation between dialects"  
echo "  - Better error handling and debugging"
echo "  - Comprehensive test coverage"
echo ""
echo "🔄 Migration complete:"
echo "  - Old regex-based parsing → SQLGlot"
echo "  - Haskell parser → Deprecated"
echo "  - Condition parser → Integrated into SQLGlot"
echo ""
echo "🚀 Run tests with: python run_parser_tests.py"