# $Id$

link_directories(${DEFAULT_LIB})

add_library(fennel_common SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${COMMON_OBJ_LIST}
${SYNCH_OBJ_LIST}
)
target_link_libraries(
fennel_common 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${COMMON_LIB}
${SYNCH_LIB}
${LINK_NO_WHOLE_ARCHIVE}
)

set(DEVICE_DEPS fennel_common)

add_library(fennel_device SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${DEVICE_OBJ_LIST} 
)
target_link_libraries(
fennel_device 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${DEVICE_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${DEVICE_DEPS})

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
  if (HAVE_LIBAIO)
    set(DEVICE_AIO_DEPS fennel_device ${DEVICE_DEPS} aio)
  else()
    set(DEVICE_AIO_DEPS fennel_device ${DEVICE_DEPS})
  endif ()
  add_library(fennel_device_aio SHARED ${fennel_SOURCE_DIR}/dummy.cpp)
  target_link_libraries(
  fennel_device_aio 
  ${PREVENT_UNDEFINED}
  ${STD_LIBS}
  ${LINK_WHOLE_ARCHIVE}
  device_aio
  ${LINK_NO_WHOLE_ARCHIVE}
  ${DEVICE_AIO_DEPS})
endif ()

set(CACHE_DEPS fennel_device ${DEVICE_DEPS})

add_library(fennel_cache SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${CACHE_OBJ_LIST} 
)
target_link_libraries(
fennel_cache 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${CACHE_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${CACHE_DEPS})

set(SEGMENT_DEPS fennel_cache ${CACHE_DEPS})

add_library(fennel_segment SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${SEGMENT_OBJ_LIST} 
)
target_link_libraries(
fennel_segment 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${SEGMENT_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${SEGMENT_DEPS})

set(TXN_DEPS fennel_segment ${SEGMENT_DEPS})

add_library(fennel_txn SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${TXN_OBJ_LIST} 
)
target_link_libraries(
fennel_txn 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${TXN_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${TXN_DEPS})

set(TUPLE_DEPS fennel_common)

add_library(fennel_tuple SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${TUPLE_OBJ_LIST} 
)
target_link_libraries(
fennel_tuple 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${TUPLE_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${TUPLE_DEPS})

set(EXEC_DEPS fennel_tuple fennel_txn ${TXN_DEPS})

add_library(fennel_exec SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${EXEC_OBJ_LIST} 
)
target_link_libraries(
fennel_exec 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${EXEC_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${EXEC_DEPS})

set(BTREE_DEPS fennel_txn ${TXN_DEPS} fennel_tuple ${TUPLE_DEPS})

add_library(fennel_btree SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${BTREE_OBJ_LIST}
)
target_link_libraries(
fennel_btree 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${BTREE_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${BTREE_DEPS})

set(DB_DEPS fennel_btree ${BTREE_DEPS})

add_library(fennel_db SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${DB_OBJ_LIST} 
)
target_link_libraries(
fennel_db 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${DB_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${DB_DEPS})

set(FTRS_DEPS fennel_btree fennel_exec ${BTREE_DEPS})

add_library(fennel_ftrs SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${FTRS_OBJ_LIST} 
)
target_link_libraries(
fennel_ftrs 
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${FTRS_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${FTRS_DEPS})

set(CALCULATOR_DEPS fennel_exec ${EXEC_DEPS})

add_library(fennel_calculator SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${CALCULATOR_OBJ_LIST} 
)
target_link_libraries(
fennel_calculator
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${CALCULATOR_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${CALCULATOR_DEPS} calcparser)

set(FLATFILE_DEPS ${CALCULATOR_DEPS})

add_library(fennel_flatfile SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${FLATFILE_OBJ_LIST} 
)
target_link_libraries(
fennel_flatfile
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${FLATFILE_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${FLATFILE_DEPS})

set(SORTER_DEPS ${CALCULATOR_DEPS})

add_library(fennel_sorter SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${SORTER_OBJ_LIST} 
)
target_link_libraries(
fennel_sorter
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${SORTER_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${SORTER_DEPS})

set(HASHEXE_DEPS ${CALCULATOR_DEPS})

add_library(fennel_hashexe SHARED ${fennel_SOURCE_DIR}/dummy.cpp
${HASHEXE_OBJ_LIST} 
)
target_link_libraries(
fennel_hashexe
${PREVENT_UNDEFINED}
${STD_LIBS}
${LINK_WHOLE_ARCHIVE}
${HASHEXE_LIB}
${LINK_NO_WHOLE_ARCHIVE}
${HASHEXE_DEPS})
