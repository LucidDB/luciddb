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
PagingTestBase.cpp \
TestBase.cpp \
SegmentTestBase.cpp \
SegStorageTestBase.cpp \
ThreadedTestBase.cpp

# NOTE:  tests are ordered roughly from fastest to slowest
noinst_PROGRAMS= \
ResourceTest \
LocalConditionTest \
TupleTest \
StandardTypeTest \
ExecStreamTest \
DatabaseTest \
LogicalTxnTest \
SegStreamTest \
SegPageIterTest \
RandomAccessFileDeviceTest \
CacheTest \
LinearDeviceSegmentTest \
LinearViewSegmentTest \
VersionedSegmentTest \
BTreeTxnTest \
BTreeTest

TESTS = $(noinst_PROGRAMS)

ResourceTest_SOURCES=ResourceTest.cpp

LocalConditionTest_SOURCES=LocalConditionTest.cpp

ExecStreamTest_SOURCES=ExecStreamTest.cpp

RandomAccessFileDeviceTest_SOURCES=RandomAccessFileDeviceTest.cpp

CacheTest_SOURCES=CacheTest.cpp

LinearDeviceSegmentTest_SOURCES=LinearDeviceSegmentTest.cpp

LinearViewSegmentTest_SOURCES=LinearViewSegmentTest.cpp

VersionedSegmentTest_SOURCES=VersionedSegmentTest.cpp

SegStreamTest_SOURCES=SegStreamTest.cpp

SegPageIterTest_SOURCES=SegPageIterTest.cpp

LogicalTxnTest_SOURCES=LogicalTxnTest.cpp

BTreeTest_SOURCES=BTreeTest.cpp

BTreeTxnTest_SOURCES=BTreeTxnTest.cpp

TupleTest_SOURCES=TupleTest.cpp

StandardTypeTest_SOURCES=StandardTypeTest.cpp

DatabaseTest_SOURCES=DatabaseTest.cpp

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
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_synch \
	-lfennel_common \
	-l$(STLPORT_LIB) \
	$(EXTRA_LDFLAGS)

check-local:
	if [ "!" -d .testlog ]; then \
		mkdir .testlog; \
	fi && \
	cp *.log .testlog/
