include $(top_srcdir)/am_include.mk

if STATIC_TEST_LIBRARY
TEST_LIB=fenneltest
else
TEST_LIB=fenneltestdyn
endif

# NOTE:  tests are ordered roughly from fastest to slowest
noinst_PROGRAMS= \
ExternalSortExecStreamTest \
FlatFileExecStreamTest \
LcsClusterAppendExecStreamTest \
LcsMultiClusterAppendTest \
LcsRowScanExecStreamTest \
LbmLoadBitmapTest \
LbmIndexScanTest

TESTS = $(noinst_PROGRAMS)

ExternalSortExecStreamTest_SOURCES=ExternalSortExecStreamTest.cpp
FlatFileExecStreamTest_SOURCES=FlatFileExecStreamTest.cpp
LcsClusterAppendExecStreamTest_SOURCES=LcsClusterAppendExecStreamTest.cpp
LcsMultiClusterAppendTest_SOURCES=LcsMultiClusterAppendTest.cpp
LcsRowScanExecStreamTest_SOURCES=LcsRowScanExecStreamTest.cpp
LbmLoadBitmapTest_SOURCES=LbmLoadBitmapTest.cpp
LbmIndexScanTest_SOURCES=LbmIndexScanTest.cpp

# NOTE:  have to repeat $(EXTRA_LDFLAGS) on mingw due to stupid linker.

LDADD = \
	-L$(top_builddir)/test \
	-L$(top_builddir)/libfennel \
	-L$(top_builddir)/lucidera/libfennel_lu \
	-L$(top_builddir)/disruptivetech/libfennel_dt \
	-l$(TEST_LIB) \
	-lfennel_lu_colstore \
	-lfennel_lu_flatfile \
	-lfennel_lu_sorter \
	-lfennel_ftrs \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_common \
	-lfennel_dt_calc \
	-lfennel_dt_xo \
	-l$(STLPORT_LIB) \
	$(EXTRA_LDFLAGS)

check-local:
	if [ "!" -d .testlog ]; then \
		mkdir .testlog; \
	fi && \
	touch dummy.log && \
	cp *.log .testlog/

