# $Id: //depot/aspen/cpp/aspen/Makefile.am#7 $
# Fennel is a relational database kernel.
# (C) Copyright 2004-2005 Disruptive Tech

include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=\
libfennel_common.la \
libfennel_synch.la \
libfennel_device.la \
libfennel_cache.la \
libfennel_segment.la \
libfennel_txn.la \
libfennel_tuple.la \
libfennel_exec.la \
libfennel_btree.la \
libfennel_db.la \
libfennel_xo.la \
libfennel_ftrs.la

SYNCH_DEPS = libfennel_common.la

DEVICE_DEPS = libfennel_synch.la $(SYNCH_DEPS)

CACHE_DEPS = libfennel_device.la $(DEVICE_DEPS)

SEGMENT_DEPS = libfennel_cache.la $(CACHE_DEPS)

TXN_DEPS = libfennel_segment.la $(SEGMENT_DEPS)

TUPLE_DEPS = libfennel_common.la

EXEC_DEPS = libfennel_tuple.la libfennel_txn.la $(TXN_DEPS)

BTREE_DEPS = libfennel_txn.la $(TXN_DEPS) libfennel_tuple.la $(TUPLE_DEPS)

DB_DEPS = libfennel_btree.la $(BTREE_DEPS)

XO_DEPS = libfennel_btree.la $(BTREE_DEPS)

FTRS_DEPS = libfennel_btree.la libfennel_exec.la $(BTREE_DEPS)

TEST_DEPS = libfennel_btree.la libfennel_exec.la $(BTREE_DEPS)

libfennel_common_la_SOURCES=
libfennel_common_la_LIBADD=\
	$(top_builddir)/common/libcommon.la

libfennel_synch_la_SOURCES=
libfennel_synch_la_LIBADD=\
	$(top_builddir)/synch/libsynch.la $(SYNCH_DEPS)

libfennel_device_la_SOURCES=
libfennel_device_la_LIBADD=\
	$(top_builddir)/device/libdevice.la $(DEVICE_DEPS)

libfennel_cache_la_SOURCES=
libfennel_cache_la_LIBADD=\
	$(top_builddir)/cache/libcache.la $(CACHE_DEPS)

libfennel_segment_la_SOURCES=
libfennel_segment_la_LIBADD=\
	$(top_builddir)/segment/libsegment.la $(SEGMENT_DEPS)

libfennel_txn_la_SOURCES=
libfennel_txn_la_LIBADD=\
	$(top_builddir)/txn/libtxn.la $(TXN_DEPS)

libfennel_tuple_la_SOURCES=
libfennel_tuple_la_LIBADD=\
	$(top_builddir)/tuple/libtuple.la $(TUPLE_DEPS)

libfennel_exec_la_SOURCES=
libfennel_exec_la_LIBADD=\
	$(top_builddir)/exec/libexec.la $(EXEC_DEPS)

libfennel_btree_la_SOURCES=
libfennel_btree_la_LIBADD=\
	$(top_builddir)/btree/libbtree.la $(BTREE_DEPS)

libfennel_db_la_SOURCES=
libfennel_db_la_LIBADD=\
	$(top_builddir)/db/libdb.la $(DB_DEPS)

libfennel_xo_la_SOURCES=
libfennel_xo_la_LIBADD=\
	$(top_builddir)/xo/libxo.la $(XO_DEPS)

libfennel_ftrs_la_SOURCES=
libfennel_ftrs_la_LIBADD=\
	$(top_builddir)/ftrs/libftrs.la $(FTRS_DEPS)

# Make sure that dependencies are strictly satisfied by failing on
# undefined references.
LDADD = -Wl,--no-undefined

DESIGN_DOC=LibraryDesign.cpp
EXTRA_DIST=$(DESIGN_DOC)
TAGS_DEPENDENCIES=$(DESIGN_DOC)
ETAGS_ARGS=$(DESIGN_DOC)
