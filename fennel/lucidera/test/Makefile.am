include $(top_srcdir)/am_include.mk

if STATIC_TEST_LIBRARY
TEST_LIB=fenneltest
else
TEST_LIB=fenneltestdyn
endif

# NOTE:  tests are ordered roughly from fastest to slowest
noinst_PROGRAMS= \
LcsClusterAppendExecStreamTest \
LcsMultiClusterAppendTest \
LcsClusterReplaceExecStreamTest \
LcsRowScanExecStreamTest \
LbmEntryTest \
LbmSplicerExecStreamTest \
LbmLoadBitmapTest \
LbmSearchTest \
LbmIntersectExecStreamTest \
LbmUnionExecStreamTest \
LbmMinusExecStreamTest \
LbmNormalizerExecStreamTest \
LbmSortedAggExecStreamTest \
LbmReaderTest

TESTS = $(noinst_PROGRAMS)

LcsClusterAppendExecStreamTest_SOURCES=LcsClusterAppendExecStreamTest.cpp
LcsMultiClusterAppendTest_SOURCES=LcsMultiClusterAppendTest.cpp
LcsClusterReplaceExecStreamTest_SOURCES=LcsClusterReplaceExecStreamTest.cpp
LcsRowScanExecStreamTest_SOURCES=LcsRowScanExecStreamTest.cpp
LbmEntryTest_SOURCES=LbmEntryTest.cpp
LbmSplicerExecStreamTest_SOURCES=LbmSplicerExecStreamTest.cpp 
LbmLoadBitmapTest_SOURCES=LbmLoadBitmapTest.cpp
LbmSearchTest_SOURCES=LbmExecStreamTestBase.cpp LbmSearchTest.cpp
LbmIntersectExecStreamTest_SOURCES=LbmExecStreamTestBase.cpp LbmIntersectExecStreamTest.cpp 
LbmUnionExecStreamTest_SOURCES=LbmExecStreamTestBase.cpp LbmUnionExecStreamTest.cpp
LbmMinusExecStreamTest_SOURCES=LbmExecStreamTestBase.cpp LbmMinusExecStreamTest.cpp
LbmNormalizerExecStreamTest_SOURCES=LbmExecStreamTestBase.cpp LbmNormalizerExecStreamTest.cpp
LbmSortedAggExecStreamTest_SOURCES=LbmExecStreamTestBase.cpp LbmSortedAggExecStreamTest.cpp
LbmReaderTest_SOURCES=LbmExecStreamTestBase.cpp LbmReaderTest.cpp

# NOTE:  have to repeat $(EXTRA_LDFLAGS) on mingw due to stupid linker.

LDADD = \
	-L$(top_builddir)/test \
	-L$(top_builddir)/libfennel \
	-L$(top_builddir)/lucidera/libfennel_lu \
	-l$(TEST_LIB) \
	-lfennel_lu_colstore \
	-lfennel_sorter \
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
    -l$(BOOST_THREADLIB) \
	-l$(STLPORT_LIB) \
	$(EXTRA_LDFLAGS)

check-local:
	if [ "!" -d .testlog ]; then \
		mkdir .testlog; \
	fi && \
	touch dummy.log && \
	cp *.log .testlog/

