include $(top_srcdir)/am_include.mk


# Only generate these files if they are out-of-dat w.r.t. the resource XML
# document.
FFennelResource.h FennelResource.cpp: FennelResource.xml
	@if [ "!" -z "$(ANT_HOME)" ]; then \
		ant -f buildResources.xml generate.resources; \
	else \
		if [ -z "$(SKIP_RESOURCE_GEN)" ]; then \
echo; \
echo "FennelResource.h or FennelResource.cpp is out of date with respect to"; \
echo "FennelResource.xml. In order to regenerate them, Apache Ant must be"; \
echo "installed, the ANT_HOME environment variable set, and the Mondrian"; \
echo "Resource Generator (MonRG) JAR files (mondrian-resource.jar and"; \
echo "mondrian-xom.jar) must be in your CLASSPATH environment variable. If"; \
echo "you wish to build Fennel without updating these files set"; \
echo "SKIP_RESOURCE_GEN=1 and re-run make (note: your build may still fail"; \
echo "if checked-in code uses resources that don't exist in your build due"; \
echo "to this error)."; \
echo; \
			exit 1; \
		fi \
	fi

noinst_LTLIBRARIES=libcommon.la
libcommon_la_SOURCES=\
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
SysCallExcn.cpp \
TraceSource.cpp \
TraceTarget.cpp

DESIGN_DOCS=CommonTerms.cpp
EXTRA_DIST=$(DESIGN_DOCS)
TAGS_DEPENDENCIES=$(DESIGN_DOCS)
ETAGS_ARGS=$(DESIGN_DOCS)
