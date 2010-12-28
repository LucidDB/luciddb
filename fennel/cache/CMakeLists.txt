# $Id$

include_directories(${DEFAULT_INCLUDE})

link_directories(${DEFAULT_LIB})

add_definitions(-DFENNEL_CACHE_EXPORTS)

add_library(
cache STATIC
Cache.cpp
CacheAllocator.cpp
CachePage.cpp
CacheParams.cpp
DelegatingCacheAccessor.cpp
FuzzyCheckpointSet.cpp
MappedPageListener.cpp
PagePredicate.cpp
QuotaCacheAccessor.cpp
TransactionalCacheAccessor.cpp
VMAllocator.cpp
)
