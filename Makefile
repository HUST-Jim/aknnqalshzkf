# pg_qalsh/Makefile

MODULE_big = pg_qalsh
EXTENSION = pg_qalsh      # the extensions name
DATA = pg_qalsh--1.0.sql  # script files to install
OBJS = pg_qalsh.o random.o util.o
REGRESS = pg_qalsh
PG_CFLAGS += -lm
PG_CFLAGS += -std=c99

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
