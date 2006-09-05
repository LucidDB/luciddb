include $(top_srcdir)/am_include.mk

noinst_LTLIBRARIES=libdevice.la libdevice_aio.la
libdevice_la_SOURCES=\
AioPollingScheduler.cpp \
AioSignalScheduler.cpp \
DeviceAccessScheduler.cpp \
DeviceAccessSchedulerParams.cpp \
FileDevice.cpp \
IoCompletionPortScheduler.cpp \
RandomAccessFileDevice.cpp \
RandomAccessNullDevice.cpp \
ThreadPoolScheduler.cpp

libdevice_aio_la_SOURCES=\
AioLinuxScheduler.cpp

DESIGN_DOC=DeviceDesign.cpp
EXTRA_DIST=$(DESIGN_DOC)
TAGS_DEPENDENCIES=$(DESIGN_DOC)
ETAGS_ARGS=$(DESIGN_DOC)
