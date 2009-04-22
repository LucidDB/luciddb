# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_LCS_EXPORTS)

add_library(
colstore STATIC
LcsClusterAccessBase.cpp
LcsClusterAppendExecStream.cpp
LcsClusterDump.cpp
LcsClusterNodeWriter.cpp
LcsClusterReader.cpp
LcsClusterReplaceExecStream.cpp
LcsClusterVerifier.cpp
LcsColumnReader.cpp
LcsHash.cpp
LcsRowScanBaseExecStream.cpp
LcsRowScanExecStream.cpp
)
