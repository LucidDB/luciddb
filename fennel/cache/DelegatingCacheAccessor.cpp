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
    MappedPageListener *pMappedPageListener)
{
    return pDelegate->lockPage(
        blockId,lockMode,readIfUnmapped,pMappedPageListener);
}

void DelegatingCacheAccessor::unlockPage(
    CachePage &page,
    LockMode lockMode)
{
    pDelegate->unlockPage(page,lockMode);
}

void DelegatingCacheAccessor::discardPage(
    BlockId blockId)
{
    pDelegate->discardPage(blockId);
}

void DelegatingCacheAccessor::prefetchPage(
    BlockId blockId,
    MappedPageListener *pMappedPageListener)
{
    pDelegate->prefetchPage(blockId,pMappedPageListener);
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

FENNEL_END_CPPFILE("$Id$");

// End DelegatingCacheAccessor.cpp
