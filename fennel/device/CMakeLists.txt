# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_DEVICE_EXPORTS)

add_library(
device STATIC
AioPollingScheduler.cpp
AioSignalScheduler.cpp
DeviceAccessScheduler.cpp
DeviceAccessSchedulerParams.cpp
FileDevice.cpp
IoCompletionPortScheduler.cpp
RandomAccessFileDevice.cpp
RandomAccessNullDevice.cpp
ThreadPoolScheduler.cpp
)

if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux")
  add_library(
  device_aio STATIC
  AioLinuxScheduler.cpp
  )
endif ()
