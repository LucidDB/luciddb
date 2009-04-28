# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_TXN_EXPORTS)

add_library(
txn STATIC
LogicalTxn.cpp
LogicalTxnLog.cpp
LogicalTxnParticipant.cpp
LogicalRecoveryLog.cpp
LogicalRecoveryTxn.cpp
)
