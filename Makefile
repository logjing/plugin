MODULE_big = iceberg_delta
OBJS = iceberg_delta.o ddl_hook.o fdw_handler.o fdw_modify.o \
       delta_table.o catalog.o fdw_options.o flush.o

EXTENSION = iceberg_delta
DATA = iceberg_delta--1.0.sql

# Use binarylibs GCC 10.3 toolchain
CC = /home/sjl/binarylibs/buildtools/gcc10.3/gcc/bin/g++
CXX = /home/sjl/binarylibs/buildtools/gcc10.3/gcc/bin/g++

# Project include path for internal headers
PG_CPPFLAGS += -Iinclude

# Iceberg-lite: lightweight Iceberg C++ library (must be before PGXS include)
ICEBERG_LITE_DIR = /home/sjl/iceberg-lite
BINARYLIBS_DIR = /home/sjl/binarylibs

PG_CPPFLAGS += -I$(ICEBERG_LITE_DIR)/include
PG_CPPFLAGS += -I$(ICEBERG_LITE_DIR)/third_party
PG_CPPFLAGS += -I$(BINARYLIBS_DIR)/kernel/dependency/libcurl/comm/include
PG_CPPFLAGS += -I$(BINARYLIBS_DIR)/kernel/dependency/openssl/comm/include

SHLIB_LINK += $(ICEBERG_LITE_DIR)/build/libiceberg_lite.a
SHLIB_LINK += $(BINARYLIBS_DIR)/kernel/dependency/libcurl/comm/lib/libcurl.so
SHLIB_LINK += $(BINARYLIBS_DIR)/kernel/dependency/openssl/comm/lib/libssl.so
SHLIB_LINK += $(BINARYLIBS_DIR)/kernel/dependency/openssl/comm/lib/libcrypto.so
SHLIB_LINK += -luuid

# C++17 is only needed for iceberg-lite; apply to flush.o via custom rule below
# to avoid PGXC header incompatibilities with the remaining object files.

PG_CONFIG = /home/sjl/openGauss-server/mppdb_temp_install/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# OpenGauss source headers: append AFTER PGXS so installed headers take precedence
# and only missing internal headers are picked from source.
override CPPFLAGS += -I/home/sjl/openGauss-server/src/include

# Force -fPIC instead of -fPIE for shared library
override CFLAGS := $(filter-out -fPIE,$(CFLAGS)) -fPIC
override CXXFLAGS := $(filter-out -fPIE,$(CXXFLAGS)) -fPIC
override CPPFLAGS := $(filter-out -fPIE,$(CPPFLAGS)) -fPIC

# Custom compile rule for flush.o: uses minimal PGXS driver flags to avoid
# C++17-incompatible PGXC headers pulled in by PGXS's full -I set.
FLUSH_PG_INCLUDE = /home/sjl/openGauss-server/mppdb_temp_install/include/postgresql/server
FLUSH_CXXFLAGS = -std=c++17 $(filter-out -std=%,$(CXXFLAGS)) -fPIC
FLUSH_CPPFLAGS = -Iinclude \
	-I$(ICEBERG_LITE_DIR)/include \
	-I$(ICEBERG_LITE_DIR)/third_party \
	-I$(BINARYLIBS_DIR)/kernel/dependency/libcurl/comm/include \
	-I$(BINARYLIBS_DIR)/kernel/dependency/openssl/comm/include \
	-I$(FLUSH_PG_INCLUDE) \
	-I/home/sjl/openGauss-server/src/include \
	-DSTREAMPLAN -DPGXC -DENABLE_HTAP -DOPENEULER_MAJOR \
	-DENABLE_OPENSSL3 -D_GNU_SOURCE

flush.o: flush.cpp
	$(CXX) $(FLUSH_CXXFLAGS) $(FLUSH_CPPFLAGS) -c -o $@ $<
