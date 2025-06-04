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
    echo "  On Windows: Download from https://get.haskellstack.org/"
    exit 1
fi

# Create stack.yaml if it doesn't exist
if [[ ! -f "$SCRIPT_DIR/stack.yaml" ]]; then
    echo "Creating stack.yaml..."
    cat > "$SCRIPT_DIR/stack.yaml" << 'EOF'
resolver: lts-21.21

packages:
- .

dependencies:
- base >= 4.7 && < 5
- parsec
- aeson
- text
- containers

ghc-options:
  "$locals": -Wall -Wcompat -Widentities -Wincomplete-record-updates -Wincomplete-uni-patterns -Wmissing-export-lists -Wmissing-home-modules -Wpartial-fields -Wredundant-constraints

extra-deps: []

allow-newer: true
EOF
fi

# Create package.yaml if it doesn't exist
if [[ ! -f "$SCRIPT_DIR/package.yaml" ]]; then
    echo "Creating package.yaml..."
    cat > "$SCRIPT_DIR/package.yaml" << 'EOF'
name:                hsqlparser
version:             0.1.0.0
github:              "pyHMSSQL/hsqlparser"
license:             BSD3
author:              "pyHMSSQL Team"
maintainer:          "example@example.com"
copyright:           "2024 pyHMSSQL Team"

extra-source-files:
- README.md

description:         Please see the README on GitHub at <https://github.com/pyHMSSQL/hsqlparser#readme>

dependencies:
- base >= 4.7 && < 5
- parsec
- aeson
- text
- containers

ghc-options:
- -Wall
- -Wcompat
- -Widentities
- -Wincomplete-record-updates
- -Wincomplete-uni-patterns
- -Wmissing-export-lists
- -Wmissing-home-modules
- -Wpartial-fields
- -Wredundant-constraints

library:
  source-dirs: src

executables:
  hsqlparser:
    main:                parser.hs
    source-dirs:         .
    ghc-options:
    - -threaded
    - -rtsopts
    - -with-rtsopts=-N
    dependencies:
    - hsqlparser

tests:
  hsqlparser-test:
    main:                Spec.hs
    source-dirs:         test
    ghc-options:
    - -threaded
    - -rtsopts
    - -with-rtsopts=-N
    dependencies:
    - hsqlparser
EOF
fi

cd "$SCRIPT_DIR"

echo "Installing dependencies..."
stack setup

echo "Building the parser..."
stack build

echo "Building executable..."
stack install --local-bin-path .

if [[ -f "$SCRIPT_DIR/hsqlparser" ]]; then
    echo "✅ Build successful! Parser executable created at: $SCRIPT_DIR/hsqlparser"
    echo "Testing the parser..."
    echo '{"tag": "Success", "contents": "Parser is working"}' | ./hsqlparser "SELECT * FROM test" > /dev/null 2>&1 && echo "✅ Parser test passed" || echo "⚠️  Parser test failed (this is expected if the parser is incomplete)"
else
    echo "❌ Build failed - executable not found"
    exit 1
fi