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

#ifndef Fennel_SegPageIter_Included
#define Fennel_SegPageIter_Included

#include <vector>
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

class Segment;

/**
 * When visiting a chain of pages via Segment.getPageSuccessor, SegPageIter can
 * be used to automatically initiate prefetches.  The caller
 * supplies the starting point, and the SegPageIter class takes care of reading
 * ahead in the chain via getPageSuccessor and issuing prefetch calls.
 *
 *<p>
 *
 * NOTE:  this is not an STL-style iterator.
 */
class FENNEL_SEGMENT_EXPORT SegPageIter
{
    /**
     * Maximum number of outstanding pre-fetch requests
     */
    uint prefetchPagesMax;

    /**
     * Number of successful pre-fetches before pre-fetch can be throttled back
     * up
     */
    uint prefetchThrottleRate;

    /**
     * Current slot in the prefetchQueue that needs to be populated
     */
    uint currPageSlot;

    /**
     * True if pre-fetches have been turned off
     */
    bool noPrefetch;

    /**
     * The remaining number of successful pre-fetches that need to occur before
     * the pre-fetch rate can be throttled back up
     */
    uint throttleCount;

    /**
     * If true, force the next pre-fetch request to be rejected
     */
    bool forceReject;

protected:
    /**
     * Accessor for the Segment containing the pages to be visited.
     */
    SegmentAccessor segmentAccessor;

    /**
     * PageId at which to stop iteration.
     */
    PageId endPageId;

    /**
     * Fixed-size circular queue of prefetched PageIds, indexed by iFetch.
     */
    std::vector<PageId> prefetchQueue;

    /**
     * Position in prefetchQueue.
     */
    uint iFetch;

    /**
     * Whether end of iteration has been reached by prefetch (but not
     * necessarily by fetch).
     */
    bool atEnd;

    /**
     * Current size of the pre-fetch queue
     */
    uint queueSize;

    /**
     * Number of slots available in the prefetchQueue.  May temporarily become
     * negative in the case where pre-fetches are turned off.
     */
    int nFreePageSlots;

    /**
     * Reads the pre-fetch parameters, sizes the pre-fetch queue, and
     * initializes various state variables related to the queue.
     */
    void initPrefetchQueue();

    /**
     * Pre-fetches a specified page.
     *
     * @param pageId the id of the page to be pre-fetched
     */
    void prefetchPage(PageId pageId);

public:
    /**
     * Constructor:  iterator starts out singular.
     */
    explicit SegPageIter();

    /**
     * Begins a new iteration.
     *
     * @param segmentAccessor accessor for the segment containing the pages to
     * visit
     *
     * @param beginPageId the ID of the first page to visit
     *
     * @param endPageId the ID at which to end iteration; by default, this is
     * NULL_PAGE_ID (representing the sentinel end of a chain) but the
     * iteration (and prefetch) can be stopped earlier with some other known
     * PageId; note that endPageId itself will not be prefetched
     */
    void mapRange(
        SegmentAccessor const &segmentAccessor,
        PageId beginPageId,
        PageId endPageId = NULL_PAGE_ID);

    /**
     * @return the current PageId in the iteration
     */
    PageId operator *() const
    {
        assert(!isSingular());
        return prefetchQueue[iFetch];
    }

    /**
     * Moves to the next prefetched PageId.  An assertion violation results if
     * called when positioned on endPageId.
     */
    void operator ++ ();

    /**
     * Forces the next pre-fetch request to be rejected.  Used for testing
     * purposes.
     */
    void forcePrefetchReject();

    /**
     * Aborts any iteration in progress and release all resources.
     */
    void makeSingular()
    {
        segmentAccessor.reset();
    }

    /**
     * @return true iff no iteration is in progress
     */
    bool isSingular() const
    {
        return segmentAccessor.pSegment ? false : true;
    }
};

FENNEL_END_NAMESPACE

#endif

// End SegPageIter.h
