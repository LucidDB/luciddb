include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES=libsegment.la
libsegment_la_SOURCES=\
CircularSegment.cpp \
CrcSegOutputStream.cpp \
CrcSegInputStream.cpp \
DelegatingSegment.cpp \
DynamicDelegatingSegment.cpp \
LinearDeviceSegment.cpp \
LinearViewSegment.cpp \
RandomAllocationSegmentBase.cpp \
RandomAllocationSegment.cpp \
ScratchSegment.cpp \
SegInputStream.cpp \
SegOutputStream.cpp \
Segment.cpp \
SegmentFactory.cpp \
SegPageIter.cpp \
SegStream.cpp \
SegStreamAllocation.cpp \
SnapshotRandomAllocationSegment.cpp \
SpillOutputStream.cpp \
TracingSegment.cpp \
VersionedRandomAllocationSegment.cpp \
VersionedSegment.cpp \
WALSegment.cpp

DESIGN_DOC=SegmentDesign.cpp
EXTRA_DIST=$(DESIGN_DOC)
TAGS_DEPENDENCIES=$(DESIGN_DOC)
ETAGS_ARGS=$(DESIGN_DOC)
