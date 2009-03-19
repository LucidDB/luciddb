/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2009 The Eigenbase Project
// Copyright (C) 2008-2009 SQLstream, Inc.
// Copyright (C) 2008-2009 LucidEra, Inc.
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
        blockId,lockMode,readIfUnmapped,pMappedPageListener,txnId);
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
    DelegatingCacheAccessor::unlockPage(page,lockMode,txnId);
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
