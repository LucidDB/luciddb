# $Id$

include_directories(
${DEFAULT_INCLUDE}
${JAVA_INCLUDE}
)

link_directories(${DEFAULT_LIB})

add_library(farrago_lu SHARED
NativeMethods_lu.cpp
)

target_link_libraries(
  farrago_lu
  ${PREVENT_UNDEFINED}
  ${STD_LIBS}
  ${LINK_WHOLE_ARCHIVE}
  ${LINK_NO_WHOLE_ARCHIVE}
  fennel_lu_colstore
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
)
