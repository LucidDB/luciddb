/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
class QuotaCacheAccessor : public TransactionalCacheAccessor
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
