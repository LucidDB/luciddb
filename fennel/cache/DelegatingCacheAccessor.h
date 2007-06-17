/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
        MappedPageListener *pMappedPageListener = NULL,
        TxnId txnId = IMPLICIT_TXN_ID);
    virtual void unlockPage(
        CachePage &page,LockMode lockMode,TxnId txnId = IMPLICIT_TXN_ID);
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
    virtual void setTxnId(TxnId txnId);
    virtual TxnId getTxnId() const;
};

FENNEL_END_NAMESPACE

#endif

// End DelegatingCacheAccessor.h
