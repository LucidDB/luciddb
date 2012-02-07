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

#ifndef Fennel_DynamicDelegatingSegment_Included
#define Fennel_DynamicDelegatingSegment_Included

#include "fennel/segment/Segment.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DynamicDelegatingSegment is similar to DelegatingSegment, except the
 * delegating segment can be dynamically changed during the life of the
 * segment.  As a result, we use a weak pointer to reference the delegating
 * segment so the pointer becomes singular once that delegating segment is
 * deallocated.
 */
class FENNEL_SEGMENT_EXPORT DynamicDelegatingSegment
    : public Segment
{
    WeakSegment delegateSegment;

    virtual void closeImpl();

public:
    /**
     * Constructs a new DynamicDelegatingSegment.
     *
     * @param delegatingSegment the underlying segment
     */
    explicit DynamicDelegatingSegment(
        WeakSegment delegatingSegment);

    virtual ~DynamicDelegatingSegment();

    void setDelegatingSegment(WeakSegment delegatingSegment);

    SharedSegment getDelegateSegment();

    // implement the Segment interface
    virtual BlockNum getAllocatedSizeInPages();
    virtual BlockNum getNumPagesOccupiedHighWater();
    virtual BlockNum getNumPagesExtended();
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId = ANON_PAGE_OWNER_ID);
    virtual bool ensureAllocatedSize(BlockNum nPages);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual void delegatedCheckpoint(
        Segment &delegatingSegment,CheckpointType checkpointType);
    virtual PageId updatePage(PageId pageId, bool needsTranslation = false);
    virtual MappedPageListener *getMappedPageListener(BlockId blockId);
    virtual bool isWriteVersioned();

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

// End DynamicDelegatingSegment.h
