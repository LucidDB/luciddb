# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_executable(testBacktrace testBacktrace.cpp Backtrace.cpp)

add_definitions(-DFENNEL_COMMON_EXPORTS)

add_library(
common STATIC
Backtrace.cpp
BernoulliRng.cpp
ByteArrayInputStream.cpp
ByteArrayOutputStream.cpp
ByteBuffer.cpp
ByteInputStream.cpp
ByteOutputStream.cpp
ByteStream.cpp
ClosableObject.cpp
ConfigMap.cpp
DataVisitor.cpp
FennelExcn.cpp
FennelResource.cpp
FileSystem.cpp
FileStatsTarget.cpp
InvalidParamExcn.cpp
Locale.cpp
Memory.cpp
PseudoUuid.cpp
ResourceBundle.cpp
ResourceDefinition.cpp
RawIntrusiveList.cpp
SizeBuffer.cpp
SysCallExcn.cpp
TraceSource.cpp
TraceTarget.cpp
)

target_link_libraries(
testBacktrace 
${STD_LIBS}
common
)

add_custom_target(
generateResources
generateResources.sh)
