include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES=libexec.la
libexec_la_SOURCES=\
AggComputer.cpp \
CartesianJoinExecStream.cpp \
ConduitExecStream.cpp \
ConfluenceExecStream.cpp \
CopyExecStream.cpp \
DfsTreeExecStreamScheduler.cpp \
DynamicParam.cpp \
ExecStream.cpp \
ExecStreamGraph.cpp \
ExecStreamGraphEmbryo.cpp \
ExecStreamScheduler.cpp \
MockConsumerExecStream.cpp \
MockProducerExecStream.cpp \
ScratchBufferExecStream.cpp \
SegBufferExecStream.cpp \
SingleInputExecStream.cpp \
SingleOutputExecStream.cpp \
SortedAggExecStream.cpp

DESIGN_DOCS=\
ExecStreamDesign.cpp \
ExecStreamHowTo.cpp \
SchedulerDesign.cpp

EXTRA_DIST=$(DESIGN_DOCS)
TAGS_DEPENDENCIES=$(DESIGN_DOCS)
ETAGS_ARGS=$(DESIGN_DOCS)
