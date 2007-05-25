include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=\
libfennel_lu_flatfile.la \
libfennel_lu_sorter.la \
libfennel_lu_colstore.la \
libfennel_lu_hashexe.la

libfennel_lu_flatfile_la_SOURCES=
libfennel_lu_flatfile_la_LIBADD=\
	$(top_builddir)/lucidera/flatfile/libflatfile.la \
	-L$(top_builddir)/libfennel \
	-lfennel_device \
	-lfennel_exec \
	-lfennel_segment \
	-lfennel_tuple \
	-lfennel_cache \
	-lfennel_common

libfennel_lu_sorter_la_SOURCES=
libfennel_lu_sorter_la_LIBADD=\
	$(top_builddir)/lucidera/sorter/libsorter.la \
	-L$(top_builddir)/libfennel \
	-lfennel_exec \
	-lfennel_segment \
	-lfennel_tuple \
	-lfennel_cache \
	-lfennel_common

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

libfennel_lu_hashexe_la_SOURCES=
libfennel_lu_hashexe_la_LIBADD=\
	$(top_builddir)/lucidera/hashexe/libhashexe.la \
	-L$(top_builddir)/libfennel \
	-lfennel_exec \
	-lfennel_segment \
	-lfennel_tuple \
	-lfennel_cache \
	-lfennel_common

# Make sure that dependencies are strictly satisfied by failing on
# undefined references.
LDADD = -Wl,--no-undefined
