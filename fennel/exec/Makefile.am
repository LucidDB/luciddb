include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES=libexec.la
libexec_la_SOURCES=\
CartesianJoinStream.cpp \
ConduitExecStream.cpp \
ConfluenceExecStream.cpp \
DfsTreeExecStreamScheduler.cpp \
ExecStream.cpp \
ExecStreamGraph.cpp \
MockProducerStream.cpp \
ScratchBufferStream.cpp \
SegBufferStream.cpp \
SourceExecStream.cpp

DESIGN_DOC=SchedulerDesign.cpp
EXTRA_DIST=$(DESIGN_DOC)
TAGS_DEPENDENCIES=$(DESIGN_DOC)
ETAGS_ARGS=$(DESIGN_DOC)
