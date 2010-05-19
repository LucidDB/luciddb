# $Id$

cmake_minimum_required(VERSION 2.6)
project(fennel)

include(CTest)
include(FindJNI)

# detect if we are a 32 or 64 bit build
if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(CPU_BITS 64)
else()
    set(CPU_BITS 32)
endif()

# TODO jvs 25-Jun-2009:  get rid of this (FNL-84)
if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
   set(TEST_LIBRARY_LINKAGE "STATIC")
else()
   set(TEST_LIBRARY_LINKAGE "SHARED")
endif()

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
   set(ARCH_DEFINES "")
else()
   if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
      set(ARCH_DEFINES "")
   else()
      # For later when we use 64-bit for Mac
      # set(ARCH_DEFINES "-arch x86_64")
      # set(CPU_BITS 64)
      set(ARCH_DEFINES "")
   endif()
endif()

set(GENERIC_DEFINES "-DHAVE_CONFIG_H -D_REENTRANT -D_STLP_USE_DYNAMIC_LIB=1 -D_STLP_NO_ANACHRONISMS=1 -DBOOST_NO_HASH=1")

if ("${OPT_FLAG}" STREQUAL "--with-optimization")
   set(CMAKE_BUILD_TYPE Release)
   if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
      set(OPT_DEFINES "")
   else()
      set(OPT_DEFINES "-Wno-unused-variable -Wno-uninitialized")
   endif()
   set(BOOST_LIB_SUFFIX "mt-p")
   set(STLPORT_LIB "stlport")
else()
   set(CMAKE_BUILD_TYPE Debug)
   set(OPT_DEFINES "-D_STLP_DEBUG=1 -D_STLP_DEBUG_UNINITIALIZED=1 -DDEBUG")
   set(BOOST_LIB_SUFFIX "mt-gdp")
   set(STLPORT_LIB "stlportstlg")
endif()

if ("${DEBUG_FLAG}" STREQUAL "--with-debug")
   if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
      set(DEBUG_DEFINES "/Zi")
   else()
      set(DEBUG_DEFINES "-g")
   endif()
else()
set(DEBUG_DEFINES "")
endif()

include(TestBigEndian)
TEST_BIG_ENDIAN(ENDIANNESS)

if (ENDIANNESS)
   set(ENDIAN_DEFINES "-DBIGENDIAN")
else()
   set(ENDIAN_DEFINES "-DLITTLEENDIAN")
endif()

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
   set(CMAKE_CXX_FLAGS "${GENERIC_DEFINES} ${ENDIAN_DEFINES}=1 ${OPT_DEFINES} ${DEBUG_DEFINES} ${ARCH_DEFINES} -D__MSVC__ -DBOOST_ALL_NO_LIB /EHsc")
   set(JDK_PLATFORM win32)
else()
   set(CMAKE_CXX_FLAGS "-pipe -ftemplate-depth-64 -Wall -Wno-unused-parameter -Wno-unused -Wno-sign-compare -Wno-parentheses -Wno-missing-braces -Werror ${GENERIC_DEFINES} ${ENDIAN_DEFINES} ${OPT_DEFINES} ${DEBUG_DEFINES} ${ARCH_DEFINES} -fPIC -DPIC")
   if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
      set(JDK_PLATFORM linux)
   else()
      set(JDK_PLATFORM darwin)
   endif()
endif()

if (NOT boost_location)
message(SEND_ERROR 
  "-Dboost_location=/path/to/boost_x_y_z must be specified")
endif()
if (NOT stlport_location)
message(SEND_ERROR 
  "-Dstlport_location=/path/to/stlport_x_y_z must be specified")
endif()

set(DEFAULT_INCLUDE
${stlport_location}/stlport
${boost_location}
${fennel_SOURCE_DIR} 
${fennel_SOURCE_DIR}/..
)

file(TO_NATIVE_PATH "$ENV{JAVA_HOME}" JAVA_HOME_NATIVE)

if (with_farrago)
   if (NOT JAVA_HOME_NATIVE)
      message(SEND_ERROR "JAVA_HOME must be set for --with-farrago build")
   endif()
endif()

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
   # FindJNI doesn't work very well on Linux
   set(JAVA_INCLUDE
      ${JAVA_HOME_NATIVE}/include
      ${JAVA_HOME_NATIVE}/include/${JDK_PLATFORM})
else()
   set(JAVA_INCLUDE ${JNI_INCLUDE_DIRS})
endif()

set(DEFAULT_LIB
${boost_location}/lib
${stlport_location}/lib
)

include (CheckIncludeFiles)
if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
   check_include_files (aio.h HAVE_AIO_H)
   check_include_files (libaio.h HAVE_LIBAIO_H)
   if (NOT with_farrago)
      check_include_files (uuid.h HAVE_UUID_H)
      check_include_files (uuid/uuid.h HAVE_UUID_UUID_H)
   endif()
endif()
check_include_files (sched.h HAVE_SCHED_H)

include(CheckFunctionExists)
check_function_exists(mmap HAVE_MMAP)
check_function_exists(mprotect HAVE_MPROTECT)

include(CheckLibraryExists)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
  check_library_exists(aio io_queue_init "" HAVE_LIBAIO)
  if (NOT HAVE_LIBAIO)
     if ("${AIO_FLAG}" STREQUAL "--with-aio-required")
        message(SEND_ERROR "libaio-dev package required but not available")
     endif()
  endif()
  if (NOT with_farrago)
     check_library_exists(uuid uuid_generate "" HAVE_LIBUUID)
     check_library_exists(uuid uuid_make "" HAVE_LIBUUID_NEW)
     if (HAVE_LIBUUID OR HAVE_LIBUUID_NEW)
        set(UUID_LINK_FLAG "-luuid")
     endif()
  endif()
endif ()

# dependency of Farrago on 64-bit jam

set(BOOST_LINKAGE runtime-link-dynamic/shared-linkable-true)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
   set(BOOST_TOOLSET msvc-stlport)
   set(DLLEXT dll)
   set(EXTRA_LDFLAGS "")
   set(boost_jam_platform cygwinx86)
   set(lib_path_var PATH)
   set(jdk_platform win32)
   set(target_os win32)
else()
   set(BOOST_TOOLSET gcc-stlport)
   set(boost_jam_platform linuxx86)
   set(jdk_platform linux)
   if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
      set(target_os linux-gnu)
      set(DLLEXT so)
      set(lib_path_var LD_LIBRARY_PATH)
      set(EXTRA_LDFLAGS 
          "-lpthread -lstdc++ -lgcc_s -lrt -ldl ${UUID_LINK_FLAG}")
   else()
      set(target_os darwin)
      set(DLLEXT dylib)
      set(lib_path_var DYLD_LIBRARY_PATH)
      set(EXTRA_LDFLAGS 
          "-lpthread -lstdc++ -ldl ${UUID_LINK_FLAG}")
   endif()
endif()

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
   set(BOOST_THREADLIB boost_thread_win32-${BOOST_LIB_SUFFIX})
else()
   set(BOOST_THREADLIB boost_thread-${BOOST_LIB_SUFFIX})
endif()
set(BOOST_REGEXLIB boost_regex-${BOOST_LIB_SUFFIX})
set(BOOST_DATETIMELIB boost_date_time-${BOOST_LIB_SUFFIX})
set(BOOST_FILESYSTEMLIB boost_filesystem-${BOOST_LIB_SUFFIX})
set(BOOST_SYSTEMLIB boost_system-${BOOST_LIB_SUFFIX})
set(BOOST_TESTLIB boost_unit_test_framework-${BOOST_LIB_SUFFIX})


macro(configure_unix_file in out)
configure_file(${in} ${out} @ONLY)
if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
  execute_process(COMMAND chmod +w ${out})
  execute_process(COMMAND dos2unix ${out})
endif()
endmacro(configure_unix_file)

configure_unix_file(
${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake
${CMAKE_CURRENT_BINARY_DIR}/config.h
)

configure_unix_file(
${CMAKE_CURRENT_SOURCE_DIR}/build/defineVariables.sh.in
${CMAKE_CURRENT_SOURCE_DIR}/build/defineVariables.sh
@ONLY
)

configure_unix_file(
${CMAKE_CURRENT_SOURCE_DIR}/fennelenv.sh.in
${CMAKE_CURRENT_SOURCE_DIR}/fennelenv.sh
@ONLY
)

set(STL_BOOST_LIBS
  ${BOOST_THREADLIB}
  ${BOOST_REGEXLIB}
  ${BOOST_DATETIMELIB}
  ${BOOST_FILESYSTEMLIB}
  ${BOOST_SYSTEMLIB}
  ${STLPORT_LIB})

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
   set(STD_LIBS
     ${STL_BOOST_LIBS}
     rpcrt4.lib
     wsock32.lib
     ws2_32.lib
   )
else()
   if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
      set(STD_LIBS
        ${STL_BOOST_LIBS}
        pthread 
        stdc++ 
        gcc_s 
        rt 
        dl 
      )
   else()
      set(STD_LIBS
        ${STL_BOOST_LIBS}
        pthread 
        stdc++ 
        dl 
      )
   endif()
endif()

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
   set(PREVENT_UNDEFINED -Wl,--no-undefined)
   set(LINK_WHOLE_ARCHIVE -Wl,-whole-archive)
   set(LINK_NO_WHOLE_ARCHIVE -Wl,-no-whole-archive)
endif()


macro(add_suffix rootlist suffix)
set(outlist)
foreach(root ${${rootlist}})
list(APPEND outlist ${root}${suffix})
endforeach(root)
set(${rootlist} ${outlist})
endmacro(add_suffix)

macro(add_prefix prefix rootlist)
set(outlist)
foreach(root ${${rootlist}})
list(APPEND outlist ${prefix}${root})
endforeach(root)
set(${rootlist} ${outlist})
endmacro(add_prefix)

macro(generate_obj_list dir)
string(TOUPPER ${dir} DIR_UPPER)
if ("${CMAKE_SYSTEM_NAME}" MATCHES "Windows")
  if (CMAKE_CONFIGURATION_TYPES)
    set(${DIR_UPPER}_OBJ_DIR
      ${CMAKE_BINARY_DIR}/${dir}/${dir}.dir/${CMAKE_CFG_INTDIR}/)
  else ()
    set(${DIR_UPPER}_OBJ_DIR
      ${CMAKE_BINARY_DIR}/${dir}${CMAKE_FILES_DIRECTORY}/${dir}.dir/)
  endif ()
  get_target_property(${DIR_UPPER}_SOURCES ${dir} SOURCES)
  set(${DIR_UPPER}_OBJ_LIST ${${DIR_UPPER}_SOURCES})
  add_suffix(${DIR_UPPER}_OBJ_LIST ${CMAKE_C_OUTPUT_EXTENSION})
  add_prefix(${${DIR_UPPER}_OBJ_DIR} ${DIR_UPPER}_OBJ_LIST)
  file(MAKE_DIRECTORY ${${DIR_UPPER}_OBJ_DIR})
  foreach(file ${${DIR_UPPER}_OBJ_LIST})
    execute_process(COMMAND ${CMAKE_COMMAND} -E touch ${file})
  endforeach(file)
else()
  set(${DIR_UPPER}_LIB ${dir})
endif()
endmacro()

add_subdirectory(common)
generate_obj_list(common)

add_subdirectory(synch)
generate_obj_list(synch)

add_subdirectory(device)
generate_obj_list(device)

add_subdirectory(cache)
generate_obj_list(cache)

add_subdirectory(segment)
generate_obj_list(segment)

add_subdirectory(txn)
generate_obj_list(txn)

add_subdirectory(tuple)
generate_obj_list(tuple)

add_subdirectory(exec)
generate_obj_list(exec)

add_subdirectory(btree)
generate_obj_list(btree)

add_subdirectory(db)
generate_obj_list(db)

add_subdirectory(ftrs)
generate_obj_list(ftrs)

add_subdirectory(calculator)
generate_obj_list(calculator)

add_subdirectory(hashexe)
generate_obj_list(hashexe)

add_subdirectory(flatfile)
generate_obj_list(flatfile)

add_subdirectory(sorter)
generate_obj_list(sorter)

add_subdirectory(lcs)
generate_obj_list(lcs)

add_subdirectory(lbm)
generate_obj_list(lbm)

add_subdirectory(util)

add_subdirectory(libfennel)
add_subdirectory(test)
add_subdirectory(calctest)
add_subdirectory(ldbtest)

if (with_farrago)
add_subdirectory(farrago)
endif()

add_custom_target(checkPreambles build/checkPreambleFennel.sh)

add_custom_target(
check
build/checkPreambleFennel.sh
COMMAND ${CMAKE_CTEST_COMMAND} -V 
)

add_custom_target(
tags
etags --c++ --declarations --members `find . -name '*.cpp' -o -name '*.h'`)

add_custom_target(
emacs_cscope
rm -f cscope.*
COMMAND cscope-indexer -r)

add_custom_target(
cscope
rm -f cscope.*
COMMAND cscope-indexer -r
COMMAND rm -f cscope.out
COMMAND cscope -b -q -k)
