/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_DelegatingSegment_Included
#define Fennel_DelegatingSegment_Included

#include "fennel/segment/Segment.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DelegatingSegment is a common base class for all Segments which delegate
 * part of their behavior to another underlying Segment.
 */
class DelegatingSegment : public Segment
{
    SharedSegment pDelegateSegment;
    
    virtual void closeImpl();
    
public:
    /**
     * Constructs a new DelegatingSegment.
     *
     * @param delegateSegment the underlying segment
     */
    explicit DelegatingSegment(
        SharedSegment delegateSegment);
    
    virtual ~DelegatingSegment();

    SharedSegment const &getDelegateSegment() const
    {
        return pDelegateSegment;
    }
    
    // implement the Segment interface
    virtual BlockNum getAllocatedSizeInPages();
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId = ANON_PAGE_OWNER_ID);
    virtual bool ensureAllocatedSize(BlockNum nPages);
    virtual void deallocatePageRange(PageId startPageId,PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual void delegatedCheckpoint(
        Segment &delegatingSegment,CheckpointType checkpointType);

    // delegate the MappedPageListener interface
    virtual void notifyPageMap(CachePage &page);
    virtual void notifyPageUnmap(CachePage &page);
    virtual void notifyAfterPageRead(CachePage &page);
    virtual void notifyPageDirty(CachePage &page,bool bDataValid);
    virtual void notifyBeforePageFlush(CachePage &page);
    virtual void notifyAfterPageFlush(CachePage &page);
    virtual bool canFlushPage(CachePage &page);
};

FENNEL_END_NAMESPACE

#endif

// End DelegatingSegment.h
