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

#include "fennel/common/CommonPreamble.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CachePage.h"
#include "fennel/cache/PagePredicate.h"
#include "fennel/segment/Segment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

Segment::Segment(SharedCache pCacheInit)
    : pCache(pCacheInit)
{
    setUsablePageSize(getFullPageSize());
}

Segment::~Segment()
{
}

void Segment::closeImpl()
{
    checkpoint(CHECKPOINT_FLUSH_AND_UNMAP);
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
    delegatedCheckpoint(*this,checkpointType);
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

bool Segment::isTracingSegment() const
{
    return false;
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

FENNEL_END_CPPFILE("$Id$");

// End Segment.cpp
