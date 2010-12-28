/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008 The Eigenbase Project
// Copyright (C) 2008 SQLstream, Inc.
// Copyright (C) 2008 Dynamo BI Corporation
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
