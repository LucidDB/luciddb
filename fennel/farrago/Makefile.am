EXTRA_INCLUDES=\
	-I$(JAVA_HOME)/include \
	-I$(JAVA_HOME)/include/@jdk_platform@

include $(top_srcdir)/am_include.mk

lib_LTLIBRARIES=libfarrago.la

libfarrago_la_SOURCES=\
CmdInterpreter.cpp \
ExecStreamBuilder.cpp \
ExecStreamFactory.cpp \
Fem.cpp \
JavaErrorTarget.cpp \
JavaExcn.cpp \
JavaSinkExecStream.cpp \
JavaTraceTarget.cpp \
JavaTransformExecStream.cpp \
JniProxy.cpp \
JniPseudoUuid.cpp \
JniUtil.cpp \
NativeMethods.cpp

libfarrago_la_LIBADD = \
	-L$(top_builddir)/libfennel \
	-lfennel_ftrs \
	-lfennel_db \
	-lfennel_btree \
	-lfennel_exec \
	-lfennel_tuple \
	-lfennel_txn \
	-lfennel_segment \
	-lfennel_cache \
	-lfennel_device \
	-lfennel_common

LDADD = -Wl,--no-undefined
