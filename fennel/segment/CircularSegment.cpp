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
#include "fennel/segment/CircularSegment.h"
#include "fennel/segment/CheckpointProvider.h"

FENNEL_BEGIN_CPPFILE("$Id$");

CircularSegment::CircularSegment(
    SharedSegment pDelegateSegment,
    SharedCheckpointProvider pCheckpointProviderInit,
    PageId oldestPageId,
    PageId newestPageId)
    : DelegatingSegment(pDelegateSegment),
      pCheckpointProvider(pCheckpointProviderInit)
{
    assert(pDelegateSegment->getAllocationOrder() == LINEAR_ALLOCATION);
    nPages = DelegatingSegment::getAllocatedSizeInPages();
    if (oldestPageId == NULL_PAGE_ID) {
        assert(newestPageId == NULL_PAGE_ID);
        oldestPageNum = 0;
        nextPageNum = 0;
    } else {
        oldestPageNum = getLinearBlockNum(oldestPageId);
        if (newestPageId != NULL_PAGE_ID) {
            nextPageNum = getLinearBlockNum(newestPageId) + 1;
            assert(oldestPageNum < nextPageNum);
            assert((nextPageNum - oldestPageNum) <= nPages);
        } else {
            nextPageNum = oldestPageNum + nPages;
        }
    }

    // REVIEW:  parameterize this
    checkpointThreshold1 = nPages / 3;
    checkpointThreshold2 = 2*checkpointThreshold1;
}

CircularSegment::~CircularSegment()
{
}

BlockNum CircularSegment::getAllocatedSizeInPages()
{
    return (nextPageNum - oldestPageNum);
}

BlockId CircularSegment::translatePageId(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    BlockNum pageNum = getLinearBlockNum(pageId);
    BlockNum blockNum = pageNum % nPages;
    return DelegatingSegment::translatePageId(
        Segment::getLinearPageId(blockNum));
}

PageId CircularSegment::translateBlockId(BlockId blockId)
{
    assert(nextPageNum > oldestPageNum);
    PageId pageId = DelegatingSegment::translateBlockId(blockId);
    BlockNum blockNum = getLinearBlockNum(pageId);
    BlockNum nBlocks = nPages;
    assert(blockNum < nBlocks);
    BlockNum oldestBlockNum = oldestPageNum % nBlocks;
    BlockNum nextBlockNum = nextPageNum % nBlocks;
    BlockNum pageNum;
    if (nextBlockNum > oldestBlockNum) {
        assert(blockNum >= oldestBlockNum);
        assert(blockNum < nextBlockNum);
        pageNum = nextPageNum - (nextBlockNum - blockNum);
    } else {
        if (blockNum < nextBlockNum) {
            pageNum = nextPageNum - (nextBlockNum - blockNum);
        } else {
            assert(blockNum >= oldestBlockNum);
            pageNum = oldestPageNum + (blockNum - oldestBlockNum);
        }
    }
    return Segment::getLinearPageId(pageNum);
}

PageId CircularSegment::allocatePageId(PageOwnerId)
{
    uint nAllocated = getAllocatedSizeInPages();

    // all full
    if (nAllocated >= nPages) {
        return NULL_PAGE_ID;
    }

    if ((nAllocated == checkpointThreshold1)
        || (nAllocated == checkpointThreshold2))
    {
        // getting low, try a checkpoint
        if (pCheckpointProvider) {
            pCheckpointProvider->requestCheckpoint(CHECKPOINT_FLUSH_FUZZY);
        }
    }

    // This wraparound should never be hit in practice.  If it is, a
    // restart which truncates logs should fix it.
    assert(!isMAXU(nextPageNum + 1));

    PageId pageId = Segment::getLinearPageId(nextPageNum);
    ++nextPageNum;
    return pageId;
}

void CircularSegment::deallocatePageRange(PageId startPageId, PageId endPageId)
{
    assert(startPageId == NULL_PAGE_ID);
    if (endPageId == NULL_PAGE_ID) {
        oldestPageNum = nextPageNum;
    } else {
        assert(isPageIdAllocated(endPageId));
        BlockNum pageNum = getLinearBlockNum(endPageId);
        oldestPageNum = pageNum + 1;
    }
}

bool CircularSegment::isPageIdAllocated(PageId pageId)
{
    BlockNum pageNum = getLinearBlockNum(pageId);
    return (pageNum >= oldestPageNum) && (pageNum < nextPageNum);
}

Segment::AllocationOrder CircularSegment::getAllocationOrder() const
{
    return LINEAR_ALLOCATION;
}

PageId CircularSegment::getPageSuccessor(PageId pageId)
{
    // Don't delegate this!
    return getLinearPageSuccessor(pageId);
}

void CircularSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    // Don't delegate this!
    setLinearPageSuccessor(pageId, successorId);
}

FENNEL_END_CPPFILE("$Id$");

// End CircularSegment.cpp
