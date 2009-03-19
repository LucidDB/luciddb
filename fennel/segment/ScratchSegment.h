/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_ScratchSegment_Included
#define Fennel_ScratchSegment_Included

#include "fennel/segment/Segment.h"
#include "fennel/cache/CacheAccessor.h"
#include "fennel/synch/SynchObj.h"
#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * ScratchSegment is an implementation of Segment which allocates scratch pages
 * from the cache.  See <a href="structSegmentDesign.html#ScratchSegment">the
 * design docs</a> for more detail.
 *
 */
class ScratchSegment : public Segment, public CacheAccessor
{
    friend class SegmentFactory;

    typedef std::vector<CachePage *> PageList;
    typedef PageList::iterator PageListIter;

    /**
     * Limit on number of pages allocated, or MAXU for unlimited.
     */
    BlockNum nPagesMax;

    /**
     * Scratch pages allocated so far.
     */
    PageList pages;

    /**
     * Mutex protecting page list.
     */
    StrictMutex mutex;

    explicit ScratchSegment(
        SharedCache pCache,
        uint nPagesMax);

    // implement ClosableObject
    virtual void closeImpl();

    void clearPages();

public:

    // implementation of Segment interface
    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual void deallocatePageRange(PageId startPageId,PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual BlockNum getAllocatedSizeInPages();
    virtual BlockNum getNumPagesOccupiedHighWater();
    virtual BlockNum getNumPagesExtended();
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual AllocationOrder getAllocationOrder() const;

    // implementation of CacheAccessor interface
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
        BlockId blockId,uint nPages,
        MappedPageListener *pMappedPageListener = NULL);
    virtual void flushPage(CachePage &page,bool async);
    virtual void nicePage(CachePage &page);
    virtual SharedCache getCache();
    virtual uint getMaxLockedPages();
    virtual void setMaxLockedPages(uint nPages);
    virtual void setTxnId(TxnId txnId);
    virtual TxnId getTxnId() const;
    virtual void getPrefetchParams(
        uint &nPagesPerBatch,
        uint &nBatchPrefetches);
};

FENNEL_END_NAMESPACE

#endif

// End ScratchSegment.h
