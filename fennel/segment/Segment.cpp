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
#include "fennel/cache/Cache.h"
#include "fennel/cache/CachePage.h"
#include "fennel/cache/PagePredicate.h"
#include "fennel/segment/Segment.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/segment/SegmentMap.h"

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

void Segment::setLinearPageSuccessor(PageId pageId, PageId successorId)
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
    pCache->checkpointPages(pagePredicate, checkpointType);
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

// force references to some classes which aren't referenced elsewhere
#ifdef __MSVC__
class UnreferencedSegmentStructs
{
    SegmentMap &segmentMap;
};
#endif

FENNEL_END_CPPFILE("$Id$");

// End Segment.cpp
