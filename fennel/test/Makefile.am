include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES=libfenneltest.la

if STATIC_TEST_LIBRARY
TEST_LIB=fenneltest
else
lib_LTLIBRARIES=libfenneltestdyn.la
TEST_LIB=fenneltestdyn
libfenneltestdyn_la_SOURCES=
endif

libfenneltest_la_SOURCES=\
CacheTestBase.cpp \
PagingTestBase.cpp \
TestBase.cpp \
SegmentTestBase.cpp \
SegStorageTestBase.cpp \
ThreadedTestBase.cpp

# NOTE:  tests are ordered roughly from fastest to slowest
noinst_PROGRAMS= \
TestResource \
TestLocalCondition \
TestTuple \
TestStandardType \
TestDatabase \
TestLogicalTxn \
TestSegStream \
TestSegPageIter \
TestRandomAccessFileDevice \
TestCache \
TestLinearDeviceSegment \
TestLinearViewSegment \
TestVersionedSegment \
TestBTreeTxn \
TestBTree

TESTS = $(noinst_PROGRAMS)

TestResource_SOURCES=TestResource.cpp

TestLocalCondition_SOURCES=TestLocalCondition.cpp

TestRandomAccessFileDevice_SOURCES=TestRandomAccessFileDevice.cpp

TestCache_SOURCES=TestCache.cpp

TestLinearDeviceSegment_SOURCES=TestLinearDeviceSegment.cpp

TestLinearViewSegment_SOURCES=TestLinearViewSegment.cpp

TestVersionedSegment_SOURCES=TestVersionedSegment.cpp

TestSegStream_SOURCES=TestSegStream.cpp

TestSegPageIter_SOURCES=TestSegPageIter.cpp

TestLogicalTxn_SOURCES=TestLogicalTxn.cpp

TestBTree_SOURCES=TestBTree.cpp

TestBTreeTxn_SOURCES=TestBTreeTxn.cpp

TestTuple_SOURCES=TestTuple.cpp

TestStandardType_SOURCES=TestStandardType.cpp

TestDatabase_SOURCES=TestDatabase.cpp

if !STATIC_TEST_LIBRARY
libfenneltestdyn_la_LIBADD=libfenneltest.la
endif

# NOTE:  don't use -Wl,--no-undefined here, because when the
# test library is dynamic, boost test relies on forward refs.
# Also, have to repeat $(EXTRA_LDFLAGS) on mingw due to stupid linker.

LDADD = -L$(top_builddir)/test -l$(TEST_LIB) \
    -L$(top_builddir)/libfennel \
	-lfennel_xo \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_synch \
	-lfennel_common \
	$(EXTRA_LDFLAGS)

check-local:
	if [ "!" -d .testlog ]; then \
		mkdir .testlog; \
	fi && \
	cp *.log .testlog/