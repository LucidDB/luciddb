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

#include "fennel/common/CommonPreamble.h"
#include "fennel/cache/DelegatingCacheAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DelegatingCacheAccessor::DelegatingCacheAccessor(
    SharedCacheAccessor pDelegateInit)
    : pDelegate(pDelegateInit)
{
}

CachePage *DelegatingCacheAccessor::lockPage(
    BlockId blockId,
    LockMode lockMode,
    bool readIfUnmapped,
    MappedPageListener *pMappedPageListener,
    TxnId txnId)
{
    return pDelegate->lockPage(
        blockId,lockMode,readIfUnmapped,pMappedPageListener,txnId);
}

void DelegatingCacheAccessor::unlockPage(
    CachePage &page,
    LockMode lockMode,
    TxnId txnId)
{
    pDelegate->unlockPage(page,lockMode,txnId);
}

void DelegatingCacheAccessor::discardPage(
    BlockId blockId)
{
    pDelegate->discardPage(blockId);
}

bool DelegatingCacheAccessor::prefetchPage(
    BlockId blockId,
    MappedPageListener *pMappedPageListener)
{
    return pDelegate->prefetchPage(blockId,pMappedPageListener);
}

void DelegatingCacheAccessor::prefetchBatch(
    BlockId blockId,uint nPages,
    MappedPageListener *pMappedPageListener)
{
    pDelegate->prefetchBatch(blockId,nPages,pMappedPageListener);
}

void DelegatingCacheAccessor::flushPage(CachePage &page,bool async)
{
    pDelegate->flushPage(page,async);
}

void DelegatingCacheAccessor::nicePage(CachePage &page)
{
    pDelegate->nicePage(page);
}

SharedCache DelegatingCacheAccessor::getCache()
{
    return pDelegate->getCache();
}

uint DelegatingCacheAccessor::getMaxLockedPages()
{
    return pDelegate->getMaxLockedPages();
}

void DelegatingCacheAccessor::setMaxLockedPages(uint nPages)
{
    pDelegate->setMaxLockedPages(nPages);
}

TxnId DelegatingCacheAccessor::getTxnId() const
{
    return pDelegate->getTxnId();
}

void DelegatingCacheAccessor::setTxnId(TxnId txnId)
{
    pDelegate->setTxnId(txnId);
}

void DelegatingCacheAccessor::getPrefetchParams(
    uint &prefetchPagesMax,
    uint &prefetchThrottleRate)
{
    pDelegate->getPrefetchParams(prefetchPagesMax, prefetchThrottleRate);
}

FENNEL_END_CPPFILE("$Id$");

// End DelegatingCacheAccessor.cpp
