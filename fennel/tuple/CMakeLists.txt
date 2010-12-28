# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_TUPLE_EXPORTS)

add_library(
tuple STATIC
AttributeAccessorImpl.cpp
StandardTypeDescriptor.cpp
TupleAccessor.cpp
TupleData.cpp
TupleDescriptor.cpp
TupleOverflowExcn.cpp
TuplePrinter.cpp
TupleProjectionAccessor.cpp
TupleDataWithBuffer.cpp
UnalignedAttributeAccessor.cpp
)
