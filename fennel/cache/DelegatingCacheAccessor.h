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

#ifndef Fennel_DelegatingCacheAccessor_Included
#define Fennel_DelegatingCacheAccessor_Included

#include "fennel/cache/CacheAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DelegatingCacheAccessor is a common base class for all implementations
 * of CacheAccessor which delegate part of their behavior to another
 * underlying CacheAccessor.
 */
class FENNEL_CACHE_EXPORT DelegatingCacheAccessor : public CacheAccessor
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
    virtual bool prefetchPage(
        BlockId blockId,
        MappedPageListener *pMappedPageListener = NULL);
    virtual void prefetchBatch(
        BlockId blockId, uint nPages,
        MappedPageListener *pMappedPageListener = NULL);
    virtual void flushPage(CachePage &page,bool async);
    virtual void nicePage(CachePage &page);
    virtual SharedCache getCache();
    virtual uint getMaxLockedPages();
    virtual void setMaxLockedPages(uint nPages);
    virtual void setTxnId(TxnId txnId);
    virtual TxnId getTxnId() const;
    virtual void getPrefetchParams(
        uint &prefetchPagesMax,
        uint &prefetchThrottleRate);
    virtual uint getProcessorCacheBytes();
};

FENNEL_END_NAMESPACE

#endif

// End DelegatingCacheAccessor.h
