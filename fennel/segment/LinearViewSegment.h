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

#ifndef Fennel_LinearViewSegment_Included
#define Fennel_LinearViewSegment_Included

#include "fennel/segment/DelegatingSegment.h"
#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * LinearViewSegment is an implementation of Segment in terms of an underlying
 * Segment, which must support the get/setPageSuccessor interface.  See <a
 * href="structSegmentDesign.html#LinearViewSegment">the design docs</a> for
 * more detail.
 *
 *<p>
 *
 * Deallocation of individual pages of a LinearViewSegment is not yet supported,
 * but full truncation is.
 *
 */
class FENNEL_SEGMENT_EXPORT LinearViewSegment
    : public DelegatingSegment
{
    friend class SegmentFactory;

    std::vector<PageId> pageTable;

    LinearViewSegment(
        SharedSegment delegateSegment,
        PageId firstPageId);

public:
    virtual ~LinearViewSegment();

    /**
     * @return the starting PageId in the underlying segment
     */
    PageId getFirstPageId() const;

    // implementation of Segment interface

    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual BlockNum getAllocatedSizeInPages();
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual PageId updatePage(PageId pageId, bool needsTranslation = false);
};

FENNEL_END_NAMESPACE

#endif

// End LinearViewSegment.h
