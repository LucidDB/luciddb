# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_library(fenneltest ${TEST_LIBRARY_LINKAGE}
CacheTestBase.cpp
ExecStreamTestBase.cpp
ExecStreamUnitTestBase.cpp
ExecStreamTestSuite.cpp
PagingTestBase.cpp
TestBase.cpp
SegmentTestBase.cpp
SegStorageTestBase.cpp
SnapshotSegmentTestBase.cpp
ThreadedTestBase.cpp
CollectExecStreamTestSuite.cpp
CorrelationJoinExecStreamTestSuite.cpp
)

target_link_libraries(
fenneltest 
${STD_LIBS}
${BOOST_TESTLIB}
fennel_ftrs
fennel_db
fennel_btree
fennel_exec
fennel_tuple
fennel_txn
fennel_segment
fennel_cache
fennel_device
fennel_common
)

set_target_properties(
fenneltest PROPERTIES COMPILE_DEFINITIONS FENNEL_TEST_EXPORTS)

# when fenneltest is STATIC, build a separate fenneltestdyn as SHARED
# (TODO jvs 25-Jun-2009:  get rid of this, FNL-84)
if ("${TEST_LIBRARY_LINKAGE}" STREQUAL "STATIC")
  add_library(fenneltestdyn SHARED
    CacheTestBase.cpp
    ExecStreamTestBase.cpp
    ExecStreamUnitTestBase.cpp
    ExecStreamTestSuite.cpp
    PagingTestBase.cpp
    TestBase.cpp
    SegmentTestBase.cpp
    SegStorageTestBase.cpp
    SnapshotSegmentTestBase.cpp
    ThreadedTestBase.cpp
    CollectExecStreamTestSuite.cpp
    CorrelationJoinExecStreamTestSuite.cpp
    )
  target_link_libraries(
    fenneltestdyn
    ${STD_LIBS}
    ${BOOST_TESTLIB}
    fennel_ftrs
    fennel_db
    fennel_btree
    fennel_exec
    fennel_tuple
    fennel_txn
    fennel_segment
    fennel_cache
    fennel_device
    fennel_common
    )
  set_target_properties(
    fenneltestdyn PROPERTIES COMPILE_DEFINITIONS FENNEL_TEST_EXPORTS)
endif()

set(PROGRAMS
ExternalSortExecStreamTest
FlatFileExecStreamTest
LhxHashTableTest
LhxJoinExecStreamTest
LhxAggExecStreamTest
TestOptionsTest
ResourceTest
LocalConditionTest
TupleTest
StandardTypeTest
SparseBitmapTest
RandomAllocationSegmentTest
ExecStreamTest
ParallelExecStreamSchedulerTest
DatabaseTest
LogicalTxnTest
SegStreamTest
SegPageIterTest
SegPageEntryIterTest
RandomAccessFileDeviceTest
CacheTest
LinearDeviceSegmentTest
LinearViewSegmentTest
BackupRestoreTest
SnapshotSegmentTest
VersionedSegmentTest
ExecStreamGovernorTest
BTreeTxnTest
BTreeTest
BTreeReadersTest
BacktraceTest
PseudoUuidTest
)

set(SUITE_PROGRAMS
CorrelationJoinExecStreamTest
CollectExecStreamTest
)

set(IMPORTED_LIBS
    fenneltest
    fennel_flatfile
    fennel_hashexe
    fennel_sorter
    fennel_ftrs
    fennel_db
    fennel_btree
    fennel_exec
    fennel_tuple
    fennel_txn
    fennel_segment
    fennel_cache
    fennel_device
    fennel_common)

enable_testing()

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.cpp)
  target_link_libraries(${program} ${STD_LIBS} ${IMPORTED_LIBS})
  add_test(${program} ${program})
endforeach(program)

foreach(program ${SUITE_PROGRAMS})
  add_executable(${program} ${program}.cpp ${program}Suite.cpp)
  target_link_libraries(${program} ${STD_LIBS} ${IMPORTED_LIBS})
  add_test(${program} ${program})
endforeach(program)
