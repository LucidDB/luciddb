/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_DelegatingCacheAccessor_Included
#define Fennel_DelegatingCacheAccessor_Included

#include "fennel/cache/CacheAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DelegatingCacheAccessor is a common base class for all implementations
 * of CacheAccessor which delegate part of their behavior to another
 * underlying CacheAccessor.
 */
class DelegatingCacheAccessor : public CacheAccessor
{
    SharedCacheAccessor pDelegate;
    
public:
    /**
     * Constructor.
     *
     * @param pDelegate the underlying CacheAccessor
     */
    explicit DelegatingCacheAccessor(
        SharedCacheAccessor pDelegate);
    
    // implement the CacheAccessor interface
    virtual CachePage *lockPage(
        BlockId blockId,
        LockMode lockMode,
        bool readIfUnmapped = true,
        MappedPageListener *pMappedPageListener = NULL);
    virtual void unlockPage(CachePage &page,LockMode lockMode);
    virtual void discardPage(BlockId blockId);
    virtual void prefetchPage(
        BlockId blockId,
        MappedPageListener *pMappedPageListener = NULL);
    virtual void prefetchBatch(
        BlockId blockId,uint nPages,
        MappedPageListener *pMappedPageListener = NULL);
    virtual void flushPage(CachePage &page,bool async);
    virtual void nicePage(CachePage &page);
    virtual SharedCache getCache();
    virtual uint getMaxLockedPages();
    virtual void setMaxLockedPages(uint nPages);
};

FENNEL_END_NAMESPACE

#endif

// End DelegatingCacheAccessor.h
