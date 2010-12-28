# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_SYNCH_EXPORTS)

add_library(
synch STATIC
SynchMonitoredObject.cpp
GroupLock.cpp
StatsTimer.cpp
SynchObj.cpp
SXMutex.cpp
Thread.cpp
ThreadPool.cpp
ThreadTracker.cpp
TimerThread.cpp
)
