/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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
class SegPageIter
{
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
     * Number of pages per batch prefetch.
     */
    uint nPagesPerBatch;

    /**
     * Number of batches to prefetch.
     */
    uint nBatchPrefetches;
    
    void prefetchBatch(PageId head,uint batchNumber);
    
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
