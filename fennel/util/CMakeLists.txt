# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
   add_executable(allocFile AllocFile.cpp)
endif ()
