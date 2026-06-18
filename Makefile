MODULE_big = iceberg_delta
OBJS = iceberg_delta.o ddl_hook.o fdw_handler.o fdw_modify.o \
       delta_table.o catalog.o fdw_options.o

EXTENSION = iceberg_delta
DATA = iceberg_delta--1.0.sql

# Use binarylibs GCC 10.3 toolchain
CC = /home/sin/binarylibs/buildtools/gcc10.3/gcc/bin/g++
CXX = /home/sin/binarylibs/buildtools/gcc10.3/gcc/bin/g++

# Project include path for internal headers
PG_CPPFLAGS += -Iinclude

PG_CONFIG = /home/sin/pg_lake_delta/mppdb_temp_install/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# OpenGauss source headers: append AFTER PGXS so installed headers take precedence
# and only missing internal headers are picked from source.
override CPPFLAGS += -I/home/sin/pg_lake_delta/src/include

# Force -fPIC instead of -fPIE for shared library
override CFLAGS := $(filter-out -fPIE,$(CFLAGS)) -fPIC
override CXXFLAGS := $(filter-out -fPIE,$(CXXFLAGS)) -fPIC
override CPPFLAGS := $(filter-out -fPIE,$(CPPFLAGS)) -fPIC
