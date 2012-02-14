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
#include "fennel/cache/TransactionalCacheAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TransactionalCacheAccessor::TransactionalCacheAccessor(
    SharedCacheAccessor pDelegateInit)
    : DelegatingCacheAccessor(pDelegateInit)
{
    implicitTxnId = IMPLICIT_TXN_ID;
}

TransactionalCacheAccessor::~TransactionalCacheAccessor()
{
}

CachePage *TransactionalCacheAccessor::lockPage(
    BlockId blockId,
    LockMode lockMode,
    bool readIfUnmapped,
    MappedPageListener *pMappedPageListener,
    TxnId txnId)
{
    if (txnId == IMPLICIT_TXN_ID) {
        txnId = implicitTxnId;
    }
    CachePage *pPage = DelegatingCacheAccessor::lockPage(
        blockId, lockMode, readIfUnmapped, pMappedPageListener, txnId);
    return pPage;
}

void TransactionalCacheAccessor::unlockPage(
    CachePage &page,
    LockMode lockMode,
    TxnId txnId)
{
    if (txnId == IMPLICIT_TXN_ID) {
        txnId = implicitTxnId;
    }
    DelegatingCacheAccessor::unlockPage(page, lockMode, txnId);
}

void TransactionalCacheAccessor::setTxnId(TxnId txnId)
{
    implicitTxnId = txnId;
}

TxnId TransactionalCacheAccessor::getTxnId() const
{
    return implicitTxnId;
}

FENNEL_END_CPPFILE("$Id$");

// End TransactionalCacheAccessor.cpp
