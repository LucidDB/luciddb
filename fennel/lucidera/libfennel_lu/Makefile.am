include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=\
libfennel_lu_colstore.la

libfennel_lu_colstore_la_SOURCES=
libfennel_lu_colstore_la_LIBADD=\
	$(top_builddir)/lucidera/colstore/libcolstore.la \
	$(top_builddir)/lucidera/bitmap/libbitmap.la \
	-L$(top_builddir)/libfennel \
	-lfennel_exec \
	-lfennel_btree \
	-lfennel_ftrs \
	-lfennel_segment \
	-lfennel_tuple \
	-lfennel_cache \
	-lfennel_common

# Make sure that dependencies are strictly satisfied by failing on
# undefined references.
LDADD = -Wl,--no-undefined
