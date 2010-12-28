# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_library(
xo STATIC
CalcExcn.cpp
CalcExecStream.cpp
CollectExecStream.cpp
UncollectExecStream.cpp
CorrelationJoinExecStream.cpp
)
