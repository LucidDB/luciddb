# $Id$

link_directories(${DEFAULT_LIB})

add_library(fennel_dt_calc SHARED ${fennel_SOURCE_DIR}/dummy.cpp)
target_link_libraries(
fennel_dt_calc
-Wl,--no-undefined
${STD_LIBS}
-Wl,-whole-archive
calc
-Wl,-no-whole-archive
fennel_exec
fennel_tuple
fennel_common
)

add_library(fennel_dt_xo SHARED ${fennel_SOURCE_DIR}/dummy.cpp)
target_link_libraries(
fennel_dt_xo
-Wl,--no-undefined
${STD_LIBS}
-Wl,-whole-archive
xo
-Wl,-no-whole-archive
fennel_dt_calc
fennel_exec
fennel_segment
fennel_tuple
fennel_common
)
