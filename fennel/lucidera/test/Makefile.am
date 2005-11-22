include $(top_srcdir)/am_include.mk

if STATIC_TEST_LIBRARY
TEST_LIB=fenneltest
else
TEST_LIB=fenneltestdyn
endif

# NOTE:  tests are ordered roughly from fastest to slowest
noinst_PROGRAMS= \
ExternalSortExecStreamTest \
FlatFileExecStreamTest

TESTS = $(noinst_PROGRAMS)

ExternalSortExecStreamTest_SOURCES=ExternalSortExecStreamTest.cpp
FlatFileExecStreamTest_SOURCES=FlatFileExecStreamTest.cpp

# NOTE:  have to repeat $(EXTRA_LDFLAGS) on mingw due to stupid linker.

LDADD = \
	-L$(top_builddir)/test \
	-L$(top_builddir)/libfennel \
	-L$(top_builddir)/lucidera/libfennel_lu \
	-l$(TEST_LIB) \
	-lfennel_lu_colstore \
	-lfennel_lu_flatfile \
	-lfennel_lu_sorter \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_common \
	-l$(STLPORT_LIB) \
	$(EXTRA_LDFLAGS)

check-local:
	if [ "!" -d .testlog ]; then \
		mkdir .testlog; \
	fi && \
	touch dummy.log && \
	cp *.log .testlog/

