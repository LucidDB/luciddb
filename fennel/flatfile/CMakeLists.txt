# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_FLATFILE_EXPORTS)

add_library(
flatfile STATIC
FlatFileBuffer.cpp
FlatFileExecStreamImpl.cpp
FlatFileParser.cpp
)
