/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
#include "fennel/segment/SegPageIter.h"
#include "fennel/segment/Segment.h"
#include "fennel/cache/CacheAccessor.h"
#include "fennel/common/CompoundId.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// TODO:  old stuff (!prefetch, chain)?

// TODO:  call nicePage on pages as they pass

// TODO:  deallocate pages as they pass

// TODO:  require nPagesPerBatch to be power of 2, and use & instead of %

SegPageIter::SegPageIter()
{
    // TODO:  parameterize
    nPagesPerBatch = 4;
    nBatchPrefetches = 3;
    prefetchQueue.resize(nPagesPerBatch*nBatchPrefetches);
}

void SegPageIter::mapRange(
    SegmentAccessor const &segmentAccessorInit,
    PageId beginPageIdInit,
    PageId endPageIdInit)
{
    assert(segmentAccessorInit.pSegment);
    assert(segmentAccessorInit.pCacheAccessor);
    segmentAccessor = segmentAccessorInit;
    iFetch = 0;
    atEnd = 0;
    endPageId = endPageIdInit;
    for (uint nextBatch = 0; nextBatch < nBatchPrefetches; nextBatch++) {
        if (atEnd) {
            break;
        }
        if (nextBatch) {
            beginPageIdInit = segmentAccessor.pSegment->getPageSuccessor(
                prefetchQueue[nextBatch*nPagesPerBatch-1]);
        }
        prefetchBatch(beginPageIdInit,nextBatch);
    }
}

void SegPageIter::operator ++ ()
{
    assert(!isSingular());
    assert(**this != endPageId);
    iFetch++;
    if (iFetch % nPagesPerBatch) {
        return;
    }
    uint batchNumber = iFetch/nPagesPerBatch - 1;
    if (!atEnd) {
        int iPrev = batchNumber*nPagesPerBatch - 1;
        if (iPrev < 0) {
            iPrev += nPagesPerBatch*nBatchPrefetches;
        }
        prefetchBatch(
            segmentAccessor.pSegment->getPageSuccessor(prefetchQueue[iPrev]),
            batchNumber);
    }
    iFetch %= (nPagesPerBatch*nBatchPrefetches);
}

void SegPageIter::prefetchBatch(PageId beginPageId,uint batchNumber)
{
    uint i,start = batchNumber*nPagesPerBatch;
    prefetchQueue[start] = beginPageId;
    if (beginPageId == endPageId) {
        atEnd = 1;
        return;
    }

    // TODO:  re-enable batching, but using BlockIds rather than PageIds
    DeviceId deviceId = CompoundId::getDeviceId(beginPageId);
    BlockNum minBlockNum = CompoundId::getBlockNum(beginPageId);
    BlockNum maxBlockNum = minBlockNum;

    bool canBatch = 1;
    
    for (i = 1; i < nPagesPerBatch; i++) {
        beginPageId = segmentAccessor.pSegment->getPageSuccessor(beginPageId);
        prefetchQueue[start+i] = beginPageId;
        if (beginPageId == endPageId) {
            canBatch = 0;
            atEnd = 1;
            break;
        }
        if (canBatch) {
            if (CompoundId::getDeviceId(beginPageId) != deviceId) {
                // TODO:  batch reads for segments which span
                // devices?
                canBatch = 0;
            } else {
                BlockNum firstBlockNum = CompoundId::getBlockNum(beginPageId);
                minBlockNum = std::min(minBlockNum,firstBlockNum);
                maxBlockNum = std::max(maxBlockNum,firstBlockNum);
            }
        }
    }

    // TODO
#if 0
    if (canBatch &&
        (maxBlockNum + 1 - minBlockNum == nPagesPerBatch))
    {
        PageId pageId;
        CompoundId::setDeviceId(pageId,deviceId);
        CompoundId::setBlockNum(pageId,minBlockNum);
        pageId = segment.getPhysicalID(pageId);
        batchSlots[batchNumber] = RecordMgr::getCache().prefetchBatch(
            pageId,&segment);
        if (batchSlots[batchNumber] != -1) return;
    }
#endif
    
    for (i = 0; i < nPagesPerBatch; i++) {
        PageId pageId = prefetchQueue[start+i];
        if (pageId == endPageId) {
            break;
        }
        BlockId blockId = segmentAccessor.pSegment->translatePageId(pageId);
        segmentAccessor.pCacheAccessor->prefetchPage(
            blockId,
            segmentAccessor.pSegment.get());
    }
}

FENNEL_END_CPPFILE("$Id$");

// End SegPageIter.cpp
