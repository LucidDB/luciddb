/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/TracingSegment.h"
#include "fennel/cache/CachePage.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// NOTE:  tracing convention is TRACE_FINE for page allocation/deallocation;
// TRACE_FINER for page writes; and TRACE_FINEST for all page accesses.  Higher
// levels should not be used; see SegmentFactory::newTracingSegment for why.

TracingSegment::TracingSegment(
    SharedSegment pDelegateSegment,
    SharedTraceTarget pTraceTarget,
    std::string sourceName)
    : DelegatingSegment(pDelegateSegment),
      TraceSource(pTraceTarget,"segment."+sourceName)
{
    FENNEL_TRACE(TRACE_FINE,"constructor");
}

TracingSegment::~TracingSegment()
{
    FENNEL_TRACE(TRACE_FINE,"destructor");
}

void TracingSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    FENNEL_TRACE(
        TRACE_FINER,
        "setPageSuccessor of PageId " << std::hex << pageId
        << " to PageId " << std::hex << successorId);
    DelegatingSegment::setPageSuccessor(pageId,successorId);
}

BlockId TracingSegment::translatePageId(PageId pageId)
{
    BlockId blockId = DelegatingSegment::translatePageId(pageId);
    FENNEL_TRACE(
        TRACE_FINEST,
        "translatePageId " << std::hex << pageId << " returns BlockId "
        << std::hex << blockId);
    return blockId;
}

PageId TracingSegment::translateBlockId(BlockId blockId)
{
    PageId pageId = DelegatingSegment::translateBlockId(blockId);
    FENNEL_TRACE(
        TRACE_FINEST,
        "translateBlockId " << std::hex << blockId << " returns PageId "
        << std::hex << pageId);
    return pageId;
}

PageId TracingSegment::allocatePageId(PageOwnerId ownerId)
{
    PageId pageId = DelegatingSegment::allocatePageId(ownerId);
    FENNEL_TRACE(
        TRACE_FINE,
        "allocatePageId for PageOwnerId " << std::hex << ownerId
        << " returns PageId " << std::hex << pageId);
    return pageId;
}

bool TracingSegment::ensureAllocatedSize(BlockNum nPages)
{
    bool b = DelegatingSegment::ensureAllocatedSize(nPages);
    FENNEL_TRACE(
        TRACE_FINE,
        "ensureAllocatedSize of " << nPages << " pages"
        << " returns " << b);
    return b;
}

void TracingSegment::deallocatePageRange(PageId startPageId,PageId endPageId)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "deallocatePageRange " << std::hex << startPageId << ", "
        << std::hex << endPageId);
    DelegatingSegment::deallocatePageRange(startPageId,endPageId);
}

void TracingSegment::notifyPageMap(CachePage &page)
{
    FENNEL_TRACE(
        TRACE_FINEST,
        "notifyPageMap @" << &page << " BlockId "
        << std::hex << page.getBlockId());
    DelegatingSegment::notifyPageMap(page);
}

void TracingSegment::notifyPageUnmap(CachePage &page)
{
    FENNEL_TRACE(
        TRACE_FINEST,
        "notifyPageUnmap @" << &page << " BlockId "
        << std::hex << page.getBlockId());
    DelegatingSegment::notifyPageUnmap(page);
}

void TracingSegment::notifyAfterPageRead(CachePage &page)
{
    FENNEL_TRACE(
        TRACE_FINEST,
        "notifyAfterPageRead @" << &page << " BlockId "
        << std::hex << page.getBlockId());
    DelegatingSegment::notifyAfterPageRead(page);
}

void TracingSegment::notifyPageDirty(CachePage &page,bool bDataValid)
{
    FENNEL_TRACE(
        TRACE_FINER,
        "notifyPageDirty @" << &page << " BlockId "
        << std::hex << page.getBlockId());
    DelegatingSegment::notifyPageDirty(page,bDataValid);
}

void TracingSegment::notifyBeforePageFlush(CachePage &page)
{
    FENNEL_TRACE(
        TRACE_FINER,
        "notifyBeforePageFlush @" << &page << " BlockId "
        << std::hex << page.getBlockId());
    DelegatingSegment::notifyBeforePageFlush(page);
}

void TracingSegment::notifyAfterPageFlush(CachePage &page)
{
    FENNEL_TRACE(
        TRACE_FINER,
        "notifyAfterPageFlush @" << &page << " BlockId "
        << std::hex << page.getBlockId());
    DelegatingSegment::notifyAfterPageFlush(page);
}

void TracingSegment::delegatedCheckpoint(
    Segment &delegatingSegment,
    CheckpointType checkpointType)
{
    FENNEL_TRACE(
        TRACE_FINER,
        "checkpoint type=" << checkpointType);
    DelegatingSegment::delegatedCheckpoint(delegatingSegment,checkpointType);
}

bool TracingSegment::isTracingSegment() const
{
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End TracingSegment.cpp
