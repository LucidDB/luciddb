# $Id$

include_directories(
${DEFAULT_INCLUDE}
$ENV{JAVA_HOME}/include
$ENV{JAVA_HOME}/include/linux
)

link_directories(${DEFAULT_LIB})

add_library(farrago_dt SHARED
NativeMethods_dt.cpp
)

target_link_libraries(
  farrago_dt
  -Wl,--no-undefined
  ${STD_LIBS}
  fennel_dt_xo
  fennel_dt_calc
  farrago
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
