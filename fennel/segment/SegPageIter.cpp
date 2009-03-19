/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
}

void SegPageIter::mapRange(
    SegmentAccessor const &segmentAccessorInit,
    PageId beginPageIdInit,
    PageId endPageIdInit)
{
    assert(segmentAccessorInit.pSegment);
    assert(segmentAccessorInit.pCacheAccessor);
    segmentAccessor = segmentAccessorInit;
    initPrefetchQueue();
    endPageId = endPageIdInit;
    for (uint i = 0; i < queueSize; i++) {
        if (atEnd) {
            break;
        }
        if (i > 0) {
            beginPageIdInit = segmentAccessor.pSegment->getPageSuccessor(
                prefetchQueue[i - 1]);
        }
        prefetchPage(beginPageIdInit);
    }
}

void SegPageIter::initPrefetchQueue()
{
    segmentAccessor.pCacheAccessor->getPrefetchParams(
        prefetchPagesMax,
        prefetchThrottleRate);
    queueSize = prefetchPagesMax;
    noPrefetch = (queueSize == 0);
    // Reset the queue size so we have space to store at least one page
    if (queueSize < 1) {
        queueSize = 1;
    }
    prefetchQueue.resize(queueSize);

    nFreePageSlots = queueSize;
    currPageSlot = 0;
    iFetch = 0;
    atEnd = 0;
    throttleCount = 0;
    forceReject = false;
}

void SegPageIter::operator ++ ()
{
    assert(!isSingular());
    assert(**this != endPageId);

    // Move past the page currently at the front of the queue.
    iFetch = (iFetch + 1) % queueSize;
    ++nFreePageSlots;

    if (atEnd) {
        return;
    }

    // Pre-fetch a page to replace the page that was at the front
    // of the queue.
    int iPrev = currPageSlot - 1;
    if (iPrev < 0) {
        iPrev += queueSize;
    }
    prefetchPage(
        segmentAccessor.pSegment->getPageSuccessor(prefetchQueue[iPrev]));
}

void SegPageIter::prefetchPage(PageId pageId)
{
    // Store the page we're about to pre-fetch in the first empty slot
    // in the queue
    prefetchQueue[currPageSlot++] = pageId;
    currPageSlot %= queueSize;
    --nFreePageSlots;

    if (pageId == endPageId) {
        atEnd = 1;
        return;
    }

    // If pre-fetches are turned off, don't bothering issuing the pre-fetch
    // request.
    BlockId blockId = NULL_BLOCK_ID;
    if (!forceReject && !noPrefetch) {
        blockId = segmentAccessor.pSegment->translatePageId(pageId);
    }
    if (!forceReject &&
        (noPrefetch ||
            segmentAccessor.pCacheAccessor->prefetchPage(
                blockId,
                segmentAccessor.pSegment->getMappedPageListener(blockId))))
    {
        // If the pre-fetch rate was throttled down, then wait until we
        // reach the desired number of successful pre-fetches before
        // throttling the rate back up, one page at a time.
        if (throttleCount > 0) {
            assert(prefetchPagesMax != 0);
            if (--throttleCount == 0) {
                // At a minimum, reenable pre-fetches
                noPrefetch = false;

                // If we haven't throttled back to the max pre-fetch
                // rate, then reset the counter so we can continue
                // counting successful pre-fetches to allow the rate
                // to continue throttling up.  In the case where the
                // pre-fetch rate is a single page, no further throttling
                // is possible.
                if (prefetchPagesMax > 1) {
                    nFreePageSlots++;
                    if (nFreePageSlots < prefetchPagesMax) {
                        throttleCount = prefetchThrottleRate;
                    }
                }
            }
        }
    } else {
        // If pre-fetches aren't already disabled, then set the number of
        // pre-fetches to the number of outstanding pre-fetches by
        // disallowing any new pre-fetches until the existing ones are used.
        // If we're down to doing a single pre-fetch, then turn off
        // pre-fetches.
        if (prefetchPagesMax > 0) {
            if (nFreePageSlots > 0) {
                nFreePageSlots = 0;
            }
            if (iFetch == currPageSlot) {
                noPrefetch = true;
            }
            throttleCount = prefetchThrottleRate;
        }
        forceReject = false;
    }
}

void SegPageIter::forcePrefetchReject()
{
    forceReject = true;
}

FENNEL_END_CPPFILE("$Id$");

// End SegPageIter.cpp
