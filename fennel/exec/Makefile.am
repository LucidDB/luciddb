include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES=libexec.la
libexec_la_SOURCES=\
ConduitExecStream.cpp \
DfsTreeExecStreamScheduler.cpp \
ExecStream.cpp \
ExecStreamGraph.cpp \
MockProducerStream.cpp \
ScratchBufferStream.cpp \
SourceExecStream.cpp

DESIGN_DOC=SchedulerDesign.cpp
EXTRA_DIST=$(DESIGN_DOC)
TAGS_DEPENDENCIES=$(DESIGN_DOC)
ETAGS_ARGS=$(DESIGN_DOC)
