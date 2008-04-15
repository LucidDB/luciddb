# Note that this file does NOT include $(top_srcdir)/am_include.mk.  That's
# because we don't want to include the define AM_LDFLAGS.  Doing so would make
# these utilities dependent on shared libraries like Boost and STL.  Instead,
# we want these utilities to be standalone.

COMMON_INCLUDES=\
	-I$(stlport_location)/stlport \
    $(icu_include) \
	-I$(boost_location) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/..

INCLUDES=$(COMMON_INCLUDES) $(EXTRA_INCLUDES)

# Don't buid the preallocation utility on Windows
if !STATIC_TEST_LIBRARY
noinst_PROGRAMS = allocFile
allocFile_SOURCES=AllocFile.cpp
allocFile_LDFLAGS=$(ICU_LD_FLAGS) -avoid-version $(EXTRA_LDFLAGS)
endif
