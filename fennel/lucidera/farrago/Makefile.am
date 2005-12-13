EXTRA_INCLUDES=\
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/@jdk_platform@

include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=libfarrago_lu.la

libfarrago_lu_la_SOURCES=\
NativeMethods_lu.cpp

libfarrago_lu_la_LIBADD = \
	-L$(top_builddir)/libfennel \
	-L$(top_builddir)/farrago \
	-L$(top_builddir)/lucidera/libfennel_lu \
	-L$(top_builddir)/disruptivetech/libfennel_dt \
	-lfennel_lu_flatfile \
	-lfennel_lu_colstore \
	-lfennel_lu_sorter \
	-lfarrago \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_ftrs \
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_common \
	-lfennel_dt_calc \
	-lfennel_dt_xo

LDADD = -Wl,--no-undefined
