# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_DB_EXPORTS)

add_library(
db STATIC
CheckpointThread.cpp
Database.cpp
)
