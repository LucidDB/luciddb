# $Id$

include_directories(
${DEFAULT_INCLUDE}
${JAVA_INCLUDE}
${CMAKE_CURRENT_SOURCE_DIR}/interface
)

link_directories(${DEFAULT_LIB})

add_library(farrago_seb SHARED
NativeMethods_seb.cpp
SebCmdInterpreter.cpp
SebInsertExecStream.cpp
SebQueryExecStream.cpp
)

target_link_libraries(
  farrago_seb
  ${PREVENT_UNDEFINED}
  ${STD_LIBS}
  ${LINK_WHOLE_ARCHIVE}
  ${LINK_NO_WHOLE_ARCHIVE}
  farrago
  fennel_db
  fennel_btree
  fennel_ftrs
  fennel_exec
  fennel_tuple
  fennel_txn
  fennel_segment
  fennel_cache
  fennel_device
  fennel_common
  ${CMAKE_CURRENT_SOURCE_DIR}/interface/libscaledb.so
)
