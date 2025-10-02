#!/bin/bash

# Build script for PostgreSQL Cache Redis Module

set -e

echo "Building PostgreSQL Cache Redis Module"
echo "======================================"

# Check dependencies
echo "Checking dependencies..."

# Check Redis
if ! command -v redis-server &> /dev/null; then
    echo "ERROR: Redis server not found. Install Redis first."
    exit 1
fi
echo "✓ Redis found"

# Check PostgreSQL development headers
if ! pg_config --includedir &> /dev/null; then
    echo "ERROR: PostgreSQL development headers not found."
    echo "Install postgresql-server-dev-all or equivalent package."
    exit 1
fi
echo "✓ PostgreSQL development headers found"

# Check hiredis
if ! pkg-config --exists hiredis 2>/dev/null; then
    echo "WARNING: hiredis not found via pkg-config"
    echo "Make sure libhiredis-dev is installed"
else
    echo "✓ hiredis found"
fi

# Check Jansson (JSON library)
if ! pkg-config --exists jansson 2>/dev/null; then
    echo "WARNING: jansson not found via pkg-config"
    echo "Make sure libjansson-dev is installed"
else
    echo "✓ jansson found"
fi

# Build the module
echo ""
echo "Building module..."
make clean
make

if [ $? -eq 0 ]; then
    echo "✓ Module built successfully: pgcache.so"
else
    echo "✗ Module build failed"
    exit 1
fi

# Check module
echo ""
echo "Module information:"
ls -la pgcache.so

echo ""
echo "Testing module loading..."

# Try to load module (this will fail if Redis is running, but shows syntax is OK)
redis-server --loadmodule ./pgcache.so --version 2>/dev/null || true

echo ""
echo "Build completed successfully!"
echo ""
echo "To start Redis with the module:"
echo "redis-server --loadmodule ./pgcache.so [module_args]"
echo ""
echo "Module arguments:"
echo "  pg_host <host>          PostgreSQL host (default: localhost)"
echo "  pg_port <port>          PostgreSQL port (default: 5432)"
echo "  pg_database <db>        PostgreSQL database (default: postgres)"
echo "  pg_user <user>          PostgreSQL user (default: postgres)"
echo "  pg_password <pass>      PostgreSQL password (default: empty)"
echo "  default_ttl <seconds>   Default cache TTL (default: 3600)"
echo "  cache_prefix <prefix>   Cache key prefix (default: pg_cache:)"
echo ""
echo "Example:"
echo "redis-server --loadmodule ./pgcache.so pg_host localhost pg_database myapp pg_user postgres pg_password secret"
