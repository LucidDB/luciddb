# $Id$

link_directories(${DEFAULT_LIB})

add_library(fennel_lu_colstore SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${COLSTORE_OBJ_LIST}
${BITMAP_OBJ_LIST})
target_link_libraries(
fennel_lu_colstore
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${COLSTORE_LIB}
${BITMAP_LIB}
${LINK_NO_WHOLE_ARCHIVE}
fennel_exec
fennel_btree
fennel_ftrs
fennel_segment
fennel_tuple
fennel_cache
fennel_common
)
