/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
      TraceSource(pTraceTarget, sourceName)
{
    FENNEL_TRACE(TRACE_FINE, "constructor");
}

TracingSegment::~TracingSegment()
{
    FENNEL_TRACE(TRACE_FINE, "destructor");
}

void TracingSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    FENNEL_TRACE(
        TRACE_FINER,
        "setPageSuccessor of PageId " << std::hex << pageId
        << " to PageId " << std::hex << successorId);
    DelegatingSegment::setPageSuccessor(pageId, successorId);
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

void TracingSegment::deallocatePageRange(PageId startPageId, PageId endPageId)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "deallocatePageRange " << std::hex << startPageId << ", "
        << std::hex << endPageId);
    DelegatingSegment::deallocatePageRange(startPageId, endPageId);
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
    DelegatingSegment::notifyPageDirty(page, bDataValid);
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
    DelegatingSegment::delegatedCheckpoint(delegatingSegment, checkpointType);
}

MappedPageListener *TracingSegment::getMappedPageListener(BlockId blockId)
{
    // We need to retrieve the listener associated with the delegating
    // segment, and then map the return value back to the parent tracing
    // segment.
    MappedPageListener *pListener =
        getDelegateSegment()->getMappedPageListener(blockId);
    FENNEL_TRACE(
        TRACE_FINEST,
        "getMappedPageListener for blockId " << std::hex << blockId
            << " = " << std::hex << pListener);

    return pListener->getTracingListener();
}

MappedPageListener *TracingSegment::notifyAfterPageCheckpointFlush(
    CachePage &page)
{
    // We need to retrieve the listener associated with the delegating
    // segment, and then map the return value back to the parent tracing
    // segment.
    MappedPageListener *pListener =
        getDelegateSegment()->notifyAfterPageCheckpointFlush(page);
    if (pListener == NULL) {
        FENNEL_TRACE(
            TRACE_FINER,
            "notifyAfterPageCheckpointFlush for blockId " << std::hex
            << page.getBlockId() << " = NULL");
        return pListener;
    } else {
        FENNEL_TRACE(
            TRACE_FINER,
            "notifyAfterPageCheckpointFlush for blockId " << std::hex
            << page.getBlockId() << " = " << std::hex << pListener);
        return pListener->getTracingListener();
    }
}

bool TracingSegment::isWriteVersioned()
{
    bool b = getDelegateSegment()->isWriteVersioned();
    FENNEL_TRACE(TRACE_FINEST, "isWriteVersioned returns " << b);
    return b;
}

FENNEL_END_CPPFILE("$Id$");

// End TracingSegment.cpp
