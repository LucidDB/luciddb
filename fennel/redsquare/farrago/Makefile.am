EXTRA_INCLUDES=\
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/@jdk_platform@

include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=libfarrago_rs.la

libfarrago_rs_la_SOURCES=\
NativeMethods_rs.cpp

libfarrago_rs_la_LIBADD = \
	-L$(top_builddir)/libfennel \
	-L$(top_builddir)/farrago \
	-L$(top_builddir)/redsquare/libfennel_rs \
	-lfennel_rs_sorter \
	-lfarrago \
	-lfennel_xo \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_synch \
	-lfennel_common

LDADD = -Wl,--no-undefined
