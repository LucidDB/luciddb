EXTRA_INCLUDES=\
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/@jdk_platform@

include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=libfarrago.la

libfarrago_la_SOURCES=\
CmdInterpreter.cpp \
ExecutionStreamFactory.cpp \
Fem.cpp \
JavaExcn.cpp \
JavaTraceTarget.cpp \
JavaTupleStream.cpp \
JniProxy.cpp \
JniUtil.cpp \
NativeMethods.cpp \
ExecutionStreamBuilder.cpp

libfarrago_la_LIBADD = \
	-L$(top_builddir)/libfennel \
	-lfennel_xo \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_synch \
	-lfennel_common

LDADD = -Wl,--no-undefined
