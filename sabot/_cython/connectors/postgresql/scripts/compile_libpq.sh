#!/bin/bash
# Quick compile script for libpq_conn.pyx

set -e

cd /Users/bengamble/Sabot

echo "🔨 Compiling libpq_conn.pyx..."

# Find libpq paths
PG_CONFIG=$(which pg_config || echo "/opt/homebrew/bin/pg_config")

if [ ! -x "$PG_CONFIG" ]; then
    echo "❌ pg_config not found. Please install PostgreSQL development headers:"
    echo "   brew install postgresql@16"
    exit 1
fi

LIBPQ_INCLUDE=$($PG_CONFIG --includedir)
LIBPQ_LIB=$($PG_CONFIG --libdir)

echo "✅ Found PostgreSQL:"
echo "   Include: $LIBPQ_INCLUDE"
echo "   Lib: $LIBPQ_LIB"

# Compile with cythonize
.venv/bin/cythonize -i \
    -I vendor/arrow/python \
    sabot/_cython/connectors/postgresql/libpq_conn.pyx \
    -X language_level=3 \
    -X boundscheck=False \
    -X wraparound=False \
    -- \
    -I$LIBPQ_INCLUDE \
    -L$LIBPQ_LIB \
    -lpq

echo "✅ Compilation complete!"
echo ""
echo "Test with:"
echo "  .venv/bin/python examples/postgres_cdc/test_libpq_connection.py"
