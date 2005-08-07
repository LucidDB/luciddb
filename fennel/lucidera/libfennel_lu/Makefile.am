include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=\
libfennel_lu_sorter.la

libfennel_lu_sorter_la_SOURCES=
libfennel_lu_sorter_la_LIBADD=\
	$(top_builddir)/lucidera/sorter/libsorter.la \
	-L$(top_builddir)/libfennel \
	-lfennel_exec \
	-lfennel_segment \
	-lfennel_tuple \
	-lfennel_common

# Make sure that dependencies are strictly satisfied by failing on
# undefined references.
LDADD = -Wl,--no-undefined
