include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=\
libfennel_dt_calc.la \
libfennel_dt_xo.la

libfennel_dt_calc_la_SOURCES=
libfennel_dt_calc_la_LIBADD=\
	$(top_builddir)/disruptivetech/calc/libcalc.la \
	-L$(top_builddir)/libfennel \
	-lfennel_tuple \
	-lfennel_synch \
	-lfennel_common

libfennel_dt_xo_la_SOURCES=
libfennel_dt_xo_la_LIBADD=\
	$(top_builddir)/disruptivetech/xo/libxo.la \
	libfennel_dt_calc.la \
	-L$(top_builddir)/libfennel \
	-lfennel_xo \
	-lfennel_tuple \
	-lfennel_synch \
	-lfennel_common

# Make sure that dependencies are strictly satisfied by failing on
# undefined references.
LDADD = -Wl,--no-undefined
