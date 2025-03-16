#!/bin/bash
# build_parser.sh

# Navigate to Haskell parser directory
cd haskell-sql-parser

# Update Haskell dependencies
cabal update

# Build the parser
cabal build

# Create a symlink to make it easier to access
ln -sf dist-newstyle/build/*/ghc-*/haskell-sql-parser-*/x/sql-parser/build/sql-parser/sql-parser ../server/sqlparser

echo "Haskell SQL Parser built successfully"