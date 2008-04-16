/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CachePage.h"
#include "fennel/cache/PagePredicate.h"
#include "fennel/segment/Segment.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

Segment::Segment(SharedCache pCacheInit)
    : pCache(pCacheInit)
{
    setUsablePageSize(getFullPageSize());
}

Segment::~Segment()
{
    close();
}

void Segment::closeImpl()
{
    checkpoint(CHECKPOINT_FLUSH_AND_UNMAP);
}

SharedSegment Segment::getTracingSegment()
{
    SharedSegment sharedPtr = pTracingSegment.lock();
    if (sharedPtr && sharedPtr.get()) {
        return sharedPtr;
    } else {
        return shared_from_this();
    }
}

void Segment::setTracingSegment(WeakSegment pTracingSegmentInit)
{
    pTracingSegment = pTracingSegmentInit;
}

MappedPageListener *Segment::getTracingListener()
{
    return getTracingSegment().get();
}

void Segment::setUsablePageSize(uint cb)
{
    cbUsablePerPage = cb;
}

PConstBuffer Segment::getReadableFooter(CachePage &page)
{
    return page.getReadableData() + getUsablePageSize();
}

PBuffer Segment::getWritableFooter(CachePage &page)
{
    return page.getWritableData() + getUsablePageSize();
}

PageId Segment::getLinearPageSuccessor(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    ++pageId;
    if (!isPageIdAllocated(pageId)) {
        return NULL_PAGE_ID;
    }
    return pageId;
}

void Segment::setLinearPageSuccessor(PageId pageId,PageId successorId)
{
    assert(isPageIdAllocated(pageId));
    assert(isPageIdAllocated(successorId));
    assert(getLinearBlockNum(successorId)
           == getLinearBlockNum(pageId) + 1);
}

bool Segment::isLinearPageIdAllocated(PageId pageId)
{
    if (getLinearBlockNum(pageId) >= getAllocatedSizeInPages()) {
        return false;
    }
    return true;
}

void Segment::checkpoint(CheckpointType checkpointType)
{
    // Note that we can't use getTracingSegment() here because that method
    // references the shared ptr associated with this segment, and the
    // shared segment may have already been freed during shutdown by the
    // time this method is called.
    SharedSegment sharedPtr = pTracingSegment.lock();
    if (sharedPtr && sharedPtr.get()) {
        delegatedCheckpoint(*(sharedPtr.get()),checkpointType);
    } else {
        delegatedCheckpoint(*this,checkpointType);
    }
}

void Segment::delegatedCheckpoint(
    Segment &delegatingSegment,
    CheckpointType checkpointType)
{
    MappedPageListenerPredicate pagePredicate(delegatingSegment);
    pCache->checkpointPages(pagePredicate,checkpointType);
}

uint Segment::getFullPageSize() const
{
    return pCache->getPageSize();
}

bool Segment::ensureAllocatedSize(BlockNum nPages)
{
    while (getAllocatedSizeInPages() < nPages) {
        if (allocatePageId() == NULL_PAGE_ID) {
            return false;
        }
    }
    return true;
}

PageId Segment::updatePage(PageId pageId, bool needsTranslation)
{
    return NULL_PAGE_ID;
}

MappedPageListener *Segment::getMappedPageListener(BlockId blockId)
{
    return this;
}

bool Segment::isWriteVersioned()
{
    return false;
}

void Segment::initForUse()
{
}

FENNEL_END_CPPFILE("$Id$");

// End Segment.cpp
