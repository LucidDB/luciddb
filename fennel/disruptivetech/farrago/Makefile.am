EXTRA_INCLUDES=\
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/@jdk_platform@

include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=libfarrago_dt.la

libfarrago_dt_la_SOURCES=\
NativeMethods_dt.cpp

libfarrago_dt_la_LIBADD = \
	-L$(top_builddir)/libfennel \
	-L$(top_builddir)/farrago \
	-L$(top_builddir)/disruptivetech/libfennel_dt \
	-lfennel_dt_xo \
	-lfennel_dt_calc \
	-lfarrago \
	-lfennel_xo \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_synch \
	-lfennel_common

LDADD = -Wl,--no-undefined
