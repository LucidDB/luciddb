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

#ifndef Fennel_QuotaCacheAccessor_Included
#define Fennel_QuotaCacheAccessor_Included

#include "fennel/cache/DelegatingCacheAccessor.h"

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
 * NOTE:  QuotaCacheAccessor is not thread-safe.
 */
class QuotaCacheAccessor : public DelegatingCacheAccessor
{
    SharedQuotaCacheAccessor pSuperQuotaAccessor;
    uint maxLockedPages;
    uint nPagesLocked;

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
        MappedPageListener *pMappedPageListener = NULL);
    virtual void unlockPage(CachePage &page,LockMode lockMode);
};

FENNEL_END_NAMESPACE

#endif

// End QuotaCacheAccessor.h
