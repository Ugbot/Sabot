# Makefile for wal2arrow PostgreSQL output plugin

MODULE_big = wal2arrow
OBJS = wal2arrow.o arrow_builder.o

# PostgreSQL extension support
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Additional includes
SHLIB_LINK += -L$(shell $(PG_CONFIG) --libdir)
