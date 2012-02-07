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

#ifndef Fennel_CircularSegment_Included
#define Fennel_CircularSegment_Included

#include "fennel/segment/DelegatingSegment.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CircularSegment implements circular page allocation in terms of an
 * underlying LINEAR_ALLOCATION segment.  See <a
 * href="structSegmentDesign.html#CircularSegment">the design docs</a> for more
 * detail.
 *
 *<p>
 *
 * Deallocation of individual pages is not supported; however, the caller may
 * deallocate runs of oldest pages by calling deallocatePageRange with
 * NULL_PAGE_ID for startPageId; all pages up to and including the specified
 * endPageId will be deallocated.  This must be done periodically to prevent
 * the allocation point from wrapping around to the oldest allocated page, or
 * an assertion violation results.
 */
class FENNEL_SEGMENT_EXPORT CircularSegment
    : public DelegatingSegment
{
    friend class SegmentFactory;

    SharedCheckpointProvider pCheckpointProvider;

    BlockNum oldestPageNum;

    // TODO:  change design doc diagram from newestPageId to nextPageId
    BlockNum nextPageNum;

    BlockNum nPages;

    BlockNum checkpointThreshold1, checkpointThreshold2;

    explicit CircularSegment(
        SharedSegment delegateSegment,
        SharedCheckpointProvider pCheckpointProvider,
        PageId oldestPageId,
        PageId newestPageId);

public:
    virtual ~CircularSegment();

    // implement the Segment interface
    virtual BlockNum getAllocatedSizeInPages();
    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId = ANON_PAGE_OWNER_ID);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
};

FENNEL_END_NAMESPACE

#endif

// End CircularSegment.h
