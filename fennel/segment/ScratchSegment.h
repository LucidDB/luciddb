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
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual AllocationOrder getAllocationOrder() const;

    // implementation of CacheAccessor interface
    virtual CachePage *lockPage(
        BlockId blockId,
        LockMode lockMode,
        bool readIfUnmapped = true,
        MappedPageListener *pMappedPageListener = NULL);
    virtual void unlockPage(CachePage &page,LockMode lockMode);
    virtual void discardPage(BlockId blockId);
    virtual void prefetchPage(
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
};

FENNEL_END_NAMESPACE

#endif

// End ScratchSegment.h
