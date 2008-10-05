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
ExecStreamTestBase.cpp \
ExecStreamUnitTestBase.cpp \
ExecStreamTestSuite.cpp \
PagingTestBase.cpp \
TestBase.cpp \
SegmentTestBase.cpp \
SegStorageTestBase.cpp \
ThreadedTestBase.cpp

# NOTE:  tests are ordered roughly from fastest to slowest
noinst_PROGRAMS= \
TestOptionsTest \
ResourceTest \
LocalConditionTest \
TupleTest \
StandardTypeTest \
RandomAllocationSegmentTest \
ExecStreamTest \
ParallelExecStreamSchedulerTest \
DatabaseTest \
LogicalTxnTest \
SegStreamTest \
SegPageIterTest \
SegPageEntryIterTest \
RandomAccessFileDeviceTest \
CacheTest \
LinearDeviceSegmentTest \
LinearViewSegmentTest \
SnapshotSegmentTest \
VersionedSegmentTest \
ExecStreamGovernorTest \
BTreeTxnTest \
BTreeTest \
BTreeReadersTest \
BacktraceTest \
PseudoUuidTest

TESTS = $(noinst_PROGRAMS)
$(noinst_PROGRAMS): $(lib_LTLIBRARIES)

TestOptionsTest_SOURCES=TestOptionsTest.cpp

ResourceTest_SOURCES=ResourceTest.cpp

LocalConditionTest_SOURCES=LocalConditionTest.cpp

ExecStreamTest_SOURCES=ExecStreamTest.cpp

ParallelExecStreamSchedulerTest_SOURCES=ParallelExecStreamSchedulerTest.cpp

RandomAccessFileDeviceTest_SOURCES=RandomAccessFileDeviceTest.cpp

CacheTest_SOURCES=CacheTest.cpp

LinearDeviceSegmentTest_SOURCES=LinearDeviceSegmentTest.cpp

LinearViewSegmentTest_SOURCES=LinearViewSegmentTest.cpp

VersionedSegmentTest_SOURCES=VersionedSegmentTest.cpp

SnapshotSegmentTest_SOURCES=SnapshotSegmentTest.cpp

SegStreamTest_SOURCES=SegStreamTest.cpp

SegPageIterTest_SOURCES=SegPageIterTest.cpp

SegPageEntryIterTest_SOURCES=SegPageEntryIterTest.cpp

LogicalTxnTest_SOURCES=LogicalTxnTest.cpp

BTreeReadersTest_SOURCES=BTreeReadersTest.cpp

BTreeTest_SOURCES=BTreeTest.cpp

BTreeTxnTest_SOURCES=BTreeTxnTest.cpp

TupleTest_SOURCES=TupleTest.cpp

StandardTypeTest_SOURCES=StandardTypeTest.cpp

DatabaseTest_SOURCES=DatabaseTest.cpp

BacktraceTest_SOURCES=BacktraceTest.cpp

ExecStreamGovernorTest_SOURCES=ExecStreamGovernorTest.cpp

PseudoUuidTest_SOURCES=PseudoUuidTest.cpp

RandomAllocationSegmentTest_SOURCES=RandomAllocationSegmentTest.cpp

if !STATIC_TEST_LIBRARY
libfenneltestdyn_la_LIBADD=libfenneltest.la
endif

# NOTE:  don't use -Wl,--no-undefined here, because when the
# test library is dynamic, boost test relies on forward refs.
# Also, have to repeat $(EXTRA_LDFLAGS) on mingw due to stupid linker.

LDADD = -L$(top_builddir)/test -l$(TEST_LIB) \
    -L$(top_builddir)/libfennel \
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
