# aknnqalshzkf/Makefile

MODULE_big = aknnqalshzkf
EXTENSION = aknnqalshzkf      # the extensions name
DATA = aknnqalshzkf--1.0.sql  # script files to install
OBJS = aknnqalshzkf.o random.o util.o
REGRESS = aknnqalshzkf
PG_CFLAGS = -std=gnu99

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
