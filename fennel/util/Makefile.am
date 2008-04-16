# Note that this file does NOT include $(top_srcdir)/am_include.mk.  That's
# because we don't want to include the define AM_LDFLAGS.  Doing so would make
# these utilities dependent on shared libraries like Boost and STL.  Instead,
# we want these utilities to be standalone.

COMMON_INCLUDES=

INCLUDES=$(COMMON_INCLUDES) $(EXTRA_INCLUDES)

# Don't buid the preallocation utility on Windows
if !STATIC_TEST_LIBRARY
noinst_PROGRAMS = allocFile
allocFile_SOURCES=AllocFile.cpp
endif
