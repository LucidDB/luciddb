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
#include "fennel/cache/QuotaCacheAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

QuotaCacheAccessor::QuotaCacheAccessor(
    SharedQuotaCacheAccessor pSuperQuotaAccessorInit,
    SharedCacheAccessor pDelegateInit,
    uint maxLockedPagesInit)
    : DelegatingCacheAccessor(pDelegateInit),
      pSuperQuotaAccessor(pSuperQuotaAccessorInit),
      maxLockedPages(maxLockedPagesInit),
      nPagesLocked(0)
{
}
    
QuotaCacheAccessor::~QuotaCacheAccessor()
{
    assert(!nPagesLocked);
}

CachePage *QuotaCacheAccessor::lockPage(
    BlockId blockId,
    LockMode lockMode,
    bool readIfUnmapped,
    MappedPageListener *pMappedPageListener)
{
    CachePage *pPage = DelegatingCacheAccessor::lockPage(
        blockId,lockMode,readIfUnmapped,pMappedPageListener);
    if (pPage) {
        incrementUsage();
    }
    return pPage;
}

void QuotaCacheAccessor::unlockPage(
    CachePage &page,
    LockMode lockMode)
{
    decrementUsage();
    DelegatingCacheAccessor::unlockPage(page,lockMode);
}

void QuotaCacheAccessor::incrementUsage()
{
    assert(nPagesLocked < maxLockedPages);
    nPagesLocked++;
    if (pSuperQuotaAccessor) {
        pSuperQuotaAccessor->incrementUsage();
    }
}

void QuotaCacheAccessor::decrementUsage()
{
    assert(nPagesLocked);
    nPagesLocked--;
    if (pSuperQuotaAccessor) {
        pSuperQuotaAccessor->decrementUsage();
    }
}

uint QuotaCacheAccessor::getMaxLockedPages()
{
    return maxLockedPages;
}

FENNEL_END_CPPFILE("$Id$");

// End QuotaCacheAccessor.cpp
