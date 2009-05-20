# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_LBM_EXPORTS)

add_library(
bitmap STATIC
LbmTupleReader.cpp
LbmSegment.cpp
LbmSegmentReaderBase.cpp
LbmSegmentReader.cpp
LbmSeqSegmentReader.cpp
LbmRidReader.cpp
LbmSegmentWriter.cpp
LbmSearchExecStream.cpp
LbmGeneratorExecStream.cpp
LbmSplicerExecStream.cpp
LbmEntry.cpp
LbmEntryDump.cpp
LbmIntersectExecStream.cpp
LbmUnionWorkspace.cpp
LbmUnionExecStream.cpp
LbmChopperExecStream.cpp
LbmBitOpExecStream.cpp
LbmMinusExecStream.cpp
LbmNormalizerExecStream.cpp
LbmByteSegment.cpp
LbmSortedAggExecStream.cpp
)
