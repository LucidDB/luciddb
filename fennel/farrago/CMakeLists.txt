# $Id$

include_directories(
${DEFAULT_INCLUDE}
${JAVA_INCLUDE}
)

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_FARRAGO_EXPORTS)

add_library(farrago SHARED
CmdInterpreter.cpp
ExecStreamBuilder.cpp
ExecStreamFactory.cpp
Fem.cpp
JavaErrorTarget.cpp
JavaExcn.cpp
JavaSinkExecStream.cpp
JavaThreadTracker.cpp
JavaTraceTarget.cpp
JavaTransformExecStream.cpp
JniProxy.cpp
JniUtil.cpp
NativeMethods.cpp
)

target_link_libraries(
  farrago
  ${PREVENT_UNDEFINED}
  ${STD_LIBS}
  fennel_calculator
  fennel_hashexe
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
