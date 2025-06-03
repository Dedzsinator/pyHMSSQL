#!/bin/bash
# filepath: /home/deginandor/Documents/Programming/pyHMSSQL/server/build_parser.sh

set -e

echo "Building Haskell SQL Parser for pyHMSSQL..."

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"

# Check if we're in the right directory (should contain stack.yaml or parser.hs)
if [[ ! -f "$SCRIPT_DIR/parser.hs" ]]; then
    echo "Error: parser.hs not found in $SCRIPT_DIR"
    echo "Please run this script from the server directory containing parser.hs"
    exit 1
fi

# Check if stack is installed
if ! command -v stack &> /dev/null; then
    echo "Error: Haskell Stack is not installed."
    echo "Please install Stack: https://docs.haskellstack.org/en/stable/install_and_upgrade/"
    echo ""
    echo "Installation instructions:"
    echo "  On Ubuntu/Debian: curl -sSL https://get.haskellstack.org/ | sh"
    echo "  On macOS: brew install haskell-stack"
    echo "  On Windows: Download from https://docs.haskellstack.org/en/stable/install_and_upgrade/"
    exit 1
fi

# Check if we need to initialize stack project
if [[ ! -f "$SCRIPT_DIR/stack.yaml" ]]; then
    echo "Initializing Stack project..."
    cd "$SCRIPT_DIR"
    stack init --resolver lts
fi

# Change to script directory for building
cd "$SCRIPT_DIR"

# Clean previous builds
echo "Cleaning previous builds..."
stack clean 2>/dev/null || true

# Build the parser
echo "Building with Stack..."
if ! stack build; then
    echo "Error: Stack build failed"
    echo "This might be due to missing dependencies or compilation errors"
    echo "Try running: stack setup"
    exit 1
fi

# Find the built executable
echo "Locating built executable..."
INSTALL_ROOT=$(stack path --local-install-root 2>/dev/null)
if [[ -z "$INSTALL_ROOT" ]]; then
    echo "Error: Could not determine stack install root"
    exit 1
fi

BINARY_PATH="$INSTALL_ROOT/bin/hsqlparser"
if [[ ! -f "$BINARY_PATH" ]]; then
    echo "Error: Built executable not found at $BINARY_PATH"
    echo "Available files in $(dirname "$BINARY_PATH"):"
    ls -la "$(dirname "$BINARY_PATH")" 2>/dev/null || echo "Directory does not exist"
    
    # Try alternative locations
    echo "Searching for hsqlparser executable..."
    find "$INSTALL_ROOT" -name "hsqlparser*" -type f 2>/dev/null || true
    
    # Try building with explicit target
    echo "Attempting to build with explicit executable target..."
    if stack build :hsqlparser; then
        BINARY_PATH=$(find "$INSTALL_ROOT" -name "hsqlparser*" -type f | head -1)
    fi
    
    if [[ ! -f "$BINARY_PATH" ]]; then
        echo "Error: Could not locate built executable"
        exit 1
    fi
fi

# Copy the binary to the project root directory
TARGET_PATH="$PROJECT_ROOT/hsqlparser"
echo "Installing binary to $TARGET_PATH..."

if cp "$BINARY_PATH" "$TARGET_PATH"; then
    chmod +x "$TARGET_PATH"
    echo "Build complete! Parser executable installed to: $TARGET_PATH"
    
    # Test the executable
    echo "Testing the parser..."
    if "$TARGET_PATH" --help >/dev/null 2>&1; then
        echo "✓ Parser executable is working correctly"
    else
        echo "⚠ Warning: Parser executable may not be working correctly"
        echo "Try running: $TARGET_PATH --help"
    fi
    
    echo ""
    echo "You can now use the Haskell parser with pyHMSSQL."
    echo "The parser will be automatically used when available."
else
    echo "Error: Failed to copy binary to $TARGET_PATH"
    exit 1
fi