include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES=libexec.la
libexec_la_SOURCES=\
CartesianJoinExecStream.cpp \
ConduitExecStream.cpp \
ConfluenceExecStream.cpp \
CopyExecStream.cpp \
DfsTreeExecStreamScheduler.cpp \
ExecStream.cpp \
ExecStreamGraph.cpp \
ExecStreamScheduler.cpp \
MockProducerExecStream.cpp \
ScratchBufferExecStream.cpp \
SegBufferExecStream.cpp \
SingleOutputExecStream.cpp

DESIGN_DOCS=SchedulerDesign.cpp ExecStreamHowTo.cpp
EXTRA_DIST=$(DESIGN_DOCS)
TAGS_DEPENDENCIES=$(DESIGN_DOCS)
ETAGS_ARGS=$(DESIGN_DOCS)
