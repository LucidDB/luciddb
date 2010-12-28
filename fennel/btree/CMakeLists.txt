# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_BTREE_EXPORTS)

add_library(
btree STATIC
BTreeAccessBase.cpp
BTreeBuilder.cpp
BTreeBuildLevel.cpp
BTreeCompactNodeAccessor.cpp
BTreeDuplicateKeyExcn.cpp
BTreeHeapNodeAccessor.cpp
BTreeLeafReader.cpp
BTreeNodeAccessor.cpp
BTreeNonLeafReader.cpp
BTreeReader.cpp
BTreeRecoveryFactory.cpp
BTreeVerifier.cpp
BTreeWriter.cpp
)
