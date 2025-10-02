# Makefile for PostgreSQL Cache Redis Module

MODULE_NAME=pgcache
MODULE_SO=$(MODULE_NAME).so

# Detect Redis installation
REDIS_CFLAGS=$(shell pkg-config --cflags hiredis 2>/dev/null || echo "-I/usr/include/hiredis")
REDIS_LDFLAGS=$(shell pkg-config --libs hiredis 2>/dev/null || echo "-lhiredis")

# Detect PostgreSQL installation
PG_CFLAGS=$(shell pg_config --includedir 2>/dev/null | sed 's/^/-I/' || echo "-I/usr/include/postgresql")
PG_LDFLAGS=$(shell pg_config --libdir 2>/dev/null | sed 's/^/-L/' || echo "-L/usr/lib") -lpq

# Detect Jansson (JSON library)
JANSSON_CFLAGS=$(shell pkg-config --cflags jansson 2>/dev/null || echo "-I/usr/include")
JANSSON_LDFLAGS=$(shell pkg-config --libs jansson 2>/dev/null || echo "-ljansson")

# Compiler and flags
CC=gcc
CFLAGS=-fPIC -std=c99 -Wall -Werror -O2 $(REDIS_CFLAGS) $(PG_CFLAGS) $(JANSSON_CFLAGS)
LDFLAGS=-shared $(REDIS_LDFLAGS) $(PG_LDFLAGS) $(JANSSON_LDFLAGS) -pthread

# Source files
SRC=src/pgcache.c
OBJ=$(SRC:.c=.o)

# Build targets
all: $(MODULE_SO)

$(MODULE_SO): $(OBJ)
	$(CC) $(LDFLAGS) -o $@ $^

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(MODULE_SO)

install: $(MODULE_SO)
	@echo "Built $(MODULE_SO)"
	@echo "To load the module in Redis, add to redis.conf:"
	@echo "loadmodule $(shell pwd)/$(MODULE_SO) [module_args]"

test: $(MODULE_SO)
	@echo "Module built successfully: $(MODULE_SO)"
	@echo "File size: $(shell ls -lh $(MODULE_SO) | awk '{print $$5}')"
	@echo "Dependencies:"
	@ldd $(MODULE_SO) 2>/dev/null || echo "Could not check dependencies"

.PHONY: all clean install test
