# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_SEGMENT_EXPORTS)

add_library(
segment STATIC
CircularSegment.cpp
CrcSegOutputStream.cpp
CrcSegInputStream.cpp
DelegatingSegment.cpp
DynamicDelegatingSegment.cpp
LinearDeviceSegment.cpp
LinearViewSegment.cpp
MockSegPageEntryIterSource.cpp
RandomAllocationSegmentBase.cpp
RandomAllocationSegment.cpp
ScratchSegment.cpp
SegInputStream.cpp
SegOutputStream.cpp
Segment.cpp
SegmentFactory.cpp
SegPageBackupRestoreDevice.cpp
SegPageIter.cpp
SegStream.cpp
SegStreamAllocation.cpp
SnapshotRandomAllocationSegment.cpp
SpillOutputStream.cpp
TracingSegment.cpp
VersionedRandomAllocationSegment.cpp
VersionedSegment.cpp
WALSegment.cpp
)
