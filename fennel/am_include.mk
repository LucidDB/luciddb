COMMON_INCLUDES=\
	-I$(stlport_location)/stlport \
	$(icu_include) \
	-I$(boost_location) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/..

INCLUDES=$(COMMON_INCLUDES) $(EXTRA_INCLUDES)

AM_LDFLAGS=\
$(ICU_LD_FLAGS) \
-L$(stlport_location)/lib -l$(STLPORT_LIB) \
-L$(BOOST_THREADLIB_DIR) -l$(BOOST_THREADLIB) \
-L$(BOOST_REGEXLIB_DIR) -l$(BOOST_REGEXLIB) \
-L$(BOOST_DATETIMELIB_DIR) -l$(BOOST_DATETIMELIB) \
$(EXTRA_LDFLAGS)

AM_ETAGSFLAGS = --c++ --members

noinst_HEADERS=$(wildcard *.h)
