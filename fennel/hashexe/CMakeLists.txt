# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_HASHEXE_EXPORTS)

add_library(
hashexe STATIC
LhxAggExecStream.cpp
LhxHashGenerator.cpp
LhxHashTable.cpp
LhxJoinExecStream.cpp
LhxPartition.cpp
)
