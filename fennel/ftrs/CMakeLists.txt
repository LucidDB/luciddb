# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_FTRS_EXPORTS)

add_library(
ftrs STATIC
BTreeExecStream.cpp
BTreeInsertExecStream.cpp
BTreePrefetchSearchExecStream.cpp
BTreeReadExecStream.cpp
BTreeScanExecStream.cpp
BTreeSearchExecStream.cpp
BTreeSearchUniqueExecStream.cpp
BTreeSortExecStream.cpp
FtrsTableWriter.cpp
FtrsTableWriterExecStream.cpp
FtrsTableWriterFactory.cpp
)
