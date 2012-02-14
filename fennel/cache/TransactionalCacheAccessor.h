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

#ifndef Fennel_TransactionalCacheAccessor_Included
#define Fennel_TransactionalCacheAccessor_Included

#include "fennel/cache/DelegatingCacheAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * TransactionalCacheAccessor implements the CacheAccessor::setTxnId method,
 * allowing it to be used to lock pages on behalf of a particular transaction
 * without the caller being aware of the association.
 *
 * @author John Sichi
 * @version $Id$
 */
class FENNEL_CACHE_EXPORT TransactionalCacheAccessor
    : public DelegatingCacheAccessor
{
    TxnId implicitTxnId;

public:
    /**
     * Constructor.
     *
     * @param pDelegate the underlying CacheAccessor
     */
    explicit TransactionalCacheAccessor(
        SharedCacheAccessor pDelegate);
    virtual ~TransactionalCacheAccessor();

    // implement the CacheAccessor interface
    virtual CachePage *lockPage(
        BlockId blockId,
        LockMode lockMode,
        bool readIfUnmapped = true,
        MappedPageListener *pMappedPageListener = NULL,
        TxnId txnId = IMPLICIT_TXN_ID);
    virtual void unlockPage(
        CachePage &page,LockMode lockMode,TxnId txnId = IMPLICIT_TXN_ID);
    virtual void setTxnId(TxnId txnId);
    virtual TxnId getTxnId() const;
};

FENNEL_END_NAMESPACE

#endif

// End TransactionalCacheAccessor.h
