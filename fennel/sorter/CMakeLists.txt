# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_SORTER_EXPORTS)

add_library(
sorter STATIC
ExternalSortExecStreamImpl.cpp
ExternalSortMerger.cpp
ExternalSortOutput.cpp
ExternalSortRunAccessor.cpp
ExternalSortRunLoader.cpp
)
