COMMON_INCLUDES=\
	-I$(stlport_location)/stlport \
	$(icu_include) \
	-I$(boost_location) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/..

INCLUDES=$(COMMON_INCLUDES) $(EXTRA_INCLUDES)

# NOTE jvs 30-June-2004:  The -Wl,-l below is to force stlport to
# come after boost; this is important on Windows, and without it,
# libtool blithely reorders them

AM_LDFLAGS=\
$(ICU_LD_FLAGS) \
-L$(boost_location)/lib \
-L$(stlport_location)/lib \
-Wl,-l$(BOOST_THREADLIB) \
-Wl,-l$(BOOST_REGEXLIB) \
-Wl,-l$(BOOST_DATETIMELIB) \
-Wl,-l$(BOOST_FILESYSTEMLIB) \
-Wl,-l$(STLPORT_LIB) \
-Wl,-E \
$(EXTRA_LDFLAGS)

AM_ETAGSFLAGS = --c++ --members

noinst_HEADERS=$(wildcard *.h)
