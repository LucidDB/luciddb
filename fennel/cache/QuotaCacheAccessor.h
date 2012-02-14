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

#ifndef Fennel_QuotaCacheAccessor_Included
#define Fennel_QuotaCacheAccessor_Included

#include "fennel/cache/TransactionalCacheAccessor.h"
#include "fennel/common/AtomicCounter.h"

FENNEL_BEGIN_NAMESPACE

class QuotaCacheAccessor;

typedef boost::shared_ptr<QuotaCacheAccessor> SharedQuotaCacheAccessor;

/**
 * QuotaCacheAccessor is an implementation of CacheAccessor which keeps
 * track of the number of locked pages and asserts that this never exceeds a
 * given quota.  It does not detect the case where the same page is locked
 * twice.
 *
 *<p>
 *
 * QuotaCacheAccessor inherits TransactionalCacheAccessor functionality.
 */
class FENNEL_CACHE_EXPORT QuotaCacheAccessor
    : public TransactionalCacheAccessor
{
    SharedQuotaCacheAccessor pSuperQuotaAccessor;
    uint maxLockedPages;
    AtomicCounter nPagesLocked;

    void incrementUsage();
    void decrementUsage();

public:
    /**
     * Constructor.
     *
     * @param pSuperQuotaAccessor if non-singular, all pages locked are also
     * charged against this super-quota; the super and sub quota limits and
     * counts are maintained independently
     *
     * @param pDelegate the underlying CacheAccessor
     *
     * @param maxLockedPages
     */
    explicit QuotaCacheAccessor(
        SharedQuotaCacheAccessor pSuperQuotaAccessor,
        SharedCacheAccessor pDelegate,
        uint maxLockedPages);

    virtual ~QuotaCacheAccessor();

    // implement CacheAccessor
    virtual uint getMaxLockedPages();

    // implement CacheAccessor
    virtual void setMaxLockedPages(uint nPages);

    /**
     * @return the current number of pages locked
     */
    uint getLockedPageCount() const
    {
        return nPagesLocked;
    }

    // implement the CacheAccessor interface
    virtual CachePage *lockPage(
        BlockId blockId,
        LockMode lockMode,
        bool readIfUnmapped = true,
        MappedPageListener *pMappedPageListener = NULL,
        TxnId txnId = IMPLICIT_TXN_ID);
    virtual void unlockPage(
        CachePage &page,LockMode lockMode,TxnId txnId = IMPLICIT_TXN_ID);
};

FENNEL_END_NAMESPACE

#endif

// End QuotaCacheAccessor.h
