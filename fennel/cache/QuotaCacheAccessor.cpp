/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/cache/QuotaCacheAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

QuotaCacheAccessor::QuotaCacheAccessor(
    SharedQuotaCacheAccessor pSuperQuotaAccessorInit,
    SharedCacheAccessor pDelegateInit,
    uint maxLockedPagesInit)
    : TransactionalCacheAccessor(pDelegateInit),
      pSuperQuotaAccessor(pSuperQuotaAccessorInit),
      maxLockedPages(maxLockedPagesInit)
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
    MappedPageListener *pMappedPageListener,
    TxnId txnId)
{
    CachePage *pPage = TransactionalCacheAccessor::lockPage(
        blockId, lockMode, readIfUnmapped, pMappedPageListener, txnId);
    if (pPage) {
        incrementUsage();
    }
    return pPage;
}

void QuotaCacheAccessor::unlockPage(
    CachePage &page,
    LockMode lockMode,
    TxnId txnId)
{
    decrementUsage();
    TransactionalCacheAccessor::unlockPage(page, lockMode, txnId);
}

void QuotaCacheAccessor::incrementUsage()
{
    assert(nPagesLocked < maxLockedPages);
    ++nPagesLocked;
    if (pSuperQuotaAccessor) {
        pSuperQuotaAccessor->incrementUsage();
    }
}

void QuotaCacheAccessor::decrementUsage()
{
    assert(nPagesLocked);
    --nPagesLocked;
    if (pSuperQuotaAccessor) {
        pSuperQuotaAccessor->decrementUsage();
    }
}

uint QuotaCacheAccessor::getMaxLockedPages()
{
    return maxLockedPages;
}

void QuotaCacheAccessor::setMaxLockedPages(uint nPages)
{
    assert(nPages >= nPagesLocked);
    maxLockedPages = nPages;
}

FENNEL_END_CPPFILE("$Id$");

// End QuotaCacheAccessor.cpp
