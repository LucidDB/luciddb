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

#ifndef Fennel_DelegatingSegment_Included
#define Fennel_DelegatingSegment_Included

#include "fennel/segment/Segment.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DelegatingSegment is a common base class for all Segments which delegate
 * part of their behavior to another underlying Segment.
 */
class FENNEL_SEGMENT_EXPORT DelegatingSegment
    : public Segment
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
