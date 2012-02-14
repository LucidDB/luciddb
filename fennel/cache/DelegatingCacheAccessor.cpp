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
        blockId, lockMode, readIfUnmapped, pMappedPageListener, txnId);
}

void DelegatingCacheAccessor::unlockPage(
    CachePage &page,
    LockMode lockMode,
    TxnId txnId)
{
    pDelegate->unlockPage(page, lockMode, txnId);
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
    return pDelegate->prefetchPage(blockId, pMappedPageListener);
}

void DelegatingCacheAccessor::prefetchBatch(
    BlockId blockId, uint nPages,
    MappedPageListener *pMappedPageListener)
{
    pDelegate->prefetchBatch(blockId, nPages, pMappedPageListener);
}

void DelegatingCacheAccessor::flushPage(CachePage &page,bool async)
{
    pDelegate->flushPage(page, async);
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

uint DelegatingCacheAccessor::getProcessorCacheBytes()
{
    return pDelegate->getProcessorCacheBytes();
}

FENNEL_END_CPPFILE("$Id$");

// End DelegatingCacheAccessor.cpp
