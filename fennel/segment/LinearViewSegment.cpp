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
#include "fennel/segment/LinearViewSegment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LinearViewSegment::LinearViewSegment(
    SharedSegment delegateSegment,
    PageId firstPageId)
    : DelegatingSegment(delegateSegment)
{
    while (firstPageId != NULL_PAGE_ID) {
        // TODO:  something more efficient
        pageTable.push_back(firstPageId);
        firstPageId = DelegatingSegment::getPageSuccessor(firstPageId);
    }
}

LinearViewSegment::~LinearViewSegment()
{
}

BlockId LinearViewSegment::translatePageId(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    BlockNum blockNum = getLinearBlockNum(pageId);
    PageId underlyingPageId = pageTable[blockNum];
    return DelegatingSegment::translatePageId(underlyingPageId);
}

PageId LinearViewSegment::translateBlockId(BlockId blockId)
{
    assert(!pageTable.empty());
    PageId underlyingPageId =
        DelegatingSegment::translateBlockId(blockId);
    std::vector<PageId>::const_iterator pFound;
    pFound = std::find(pageTable.begin(), pageTable.end(), underlyingPageId);
    assert(pFound != pageTable.end());
    return getLinearPageId(pFound - pageTable.begin());
}

BlockNum LinearViewSegment::getAllocatedSizeInPages()
{
    return pageTable.size();
}

PageId LinearViewSegment::allocatePageId(PageOwnerId ownerId)
{
    PageId underlyingPageId = DelegatingSegment::allocatePageId(ownerId);
    if (underlyingPageId == NULL_PAGE_ID) {
        return underlyingPageId;
    }
    if (!pageTable.empty()) {
        DelegatingSegment::setPageSuccessor(
            pageTable.back(), underlyingPageId);
    }
    PageId pageId = getLinearPageId(pageTable.size());
    pageTable.push_back(underlyingPageId);
    return pageId;
}

void LinearViewSegment::deallocatePageRange(
    PageId startPageId, PageId endPageId)
{
    // TODO:  support truncation with startPageId != NULL_PAGE_ID
    assert(startPageId == NULL_PAGE_ID);
    assert(endPageId == NULL_PAGE_ID);
    pageTable.clear();
}

bool LinearViewSegment::isPageIdAllocated(PageId pageId)
{
    return isLinearPageIdAllocated(pageId);
}

PageId LinearViewSegment::getFirstPageId() const
{
    if (pageTable.empty()) {
        return NULL_PAGE_ID;
    } else {
        return pageTable.front();
    }
}

PageId LinearViewSegment::getPageSuccessor(PageId pageId)
{
    return getLinearPageSuccessor(pageId);
}

void LinearViewSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    setLinearPageSuccessor(pageId, successorId);
}

Segment::AllocationOrder LinearViewSegment::getAllocationOrder() const
{
    return LINEAR_ALLOCATION;
}

PageId LinearViewSegment::updatePage(PageId pageId, bool needsTranslation)
{
    assert(isPageIdAllocated(pageId));
    BlockNum blockNum = getLinearBlockNum(pageId);
    PageId underlyingPageId = pageTable[blockNum];
    return DelegatingSegment::updatePage(underlyingPageId, needsTranslation);
}

FENNEL_END_CPPFILE("$Id$");

// End LinearViewSegment.cpp
