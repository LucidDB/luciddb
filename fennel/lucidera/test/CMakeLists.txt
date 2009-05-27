# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

set(PROGRAMS
LcsClusterAppendExecStreamTest
LcsMultiClusterAppendTest
LcsClusterReplaceExecStreamTest
LcsRowScanExecStreamTest
LbmEntryTest
LbmSplicerExecStreamTest
LbmLoadBitmapTest
)

set(LBM_BASE_PROGRAMS
LbmSearchTest
LbmIntersectExecStreamTest
LbmUnionExecStreamTest
LbmMinusExecStreamTest
LbmNormalizerExecStreamTest
LbmSortedAggExecStreamTest
LbmReaderTest
)

set(IMPORTED_LIBS
  fenneltest
  fennel_hashexe
  fennel_lu_colstore
  fennel_flatfile
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
  fennel_common
)

foreach(program ${PROGRAMS})
  add_executable(${program} ${program}.cpp)
  target_link_libraries(${program}
    ${STD_LIBS}
    ${IMPORTED_LIBS})
  add_test(${program} ${program})
endforeach(program)

foreach(program ${LBM_BASE_PROGRAMS})
  add_executable(${program} ${program}.cpp LbmExecStreamTestBase.cpp)
  target_link_libraries(${program}
    ${STD_LIBS}
    ${IMPORTED_LIBS})
  add_test(${program} ${program})
endforeach(program)
