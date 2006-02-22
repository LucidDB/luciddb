include $(top_srcdir)/am_include.mk

# NOTE jvs 22-Dec-2005:  we used to try to use timestamps to
# regenerate resources automatically, but it caused too many problems
# with branch/merge.

# Regenerate resources only when explicitly requested
generateResources:
	@if [ "!" -z "$(ANT_HOME)" ]; then \
		ant -f buildResources.xml execute.resgen; \
	else \
echo; \
echo "In order to regenerate FennelResource code, Apache Ant must be"; \
echo "installed, the ANT_HOME environment variable set, and the Eigenbase"; \
echo "Resource Generator (ResGen) JAR files (eigenbase-resgen.jar and"; \
echo "eigenbase-xom.jar) must be in your CLASSPATH environment variable."; \
echo; \
exit 1; \
	fi

noinst_LTLIBRARIES=libcommon.la
libcommon_la_SOURCES=\
Backtrace.cpp \
ByteArrayInputStream.cpp \
ByteArrayOutputStream.cpp \
ByteInputStream.cpp \
ByteOutputStream.cpp \
ByteStream.cpp \
ClosableObject.cpp \
ConfigMap.cpp \
DataVisitor.cpp \
FennelExcn.cpp \
FennelResource.cpp \
FileSystem.cpp \
FileStatsTarget.cpp \
Locale.cpp \
Memory.cpp \
PseudoUuid.cpp \
ResourceBundle.cpp \
ResourceDefinition.cpp \
RawIntrusiveList.cpp \
SizeBuffer.cpp \
SysCallExcn.cpp \
TraceSource.cpp \
TraceTarget.cpp

DESIGN_DOCS=CommonTerms.cpp
EXTRA_DIST=$(DESIGN_DOCS)
TAGS_DEPENDENCIES=$(DESIGN_DOCS)
ETAGS_ARGS=$(DESIGN_DOCS)

# a trivial standalone test of Backtrace
# TODO jvs 26-June-2005:  This is a hack to suppress build on Windows;
# if we ever get backtrace working on Windows, need to come up
# with the proper link line.
if !STATIC_TEST_LIBRARY
noinst_PROGRAMS = testBacktrace
testBacktrace_SOURCES=testBacktrace.cpp
testBacktrace_DEPENDENCIES=Backtrace.cpp Backtrace.h libcommon.la
testBacktrace_LDADD=-lcommon
endif

