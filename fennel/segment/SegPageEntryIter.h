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

#ifndef Fennel_SegPageEntryIter_Included
#define Fennel_SegPageEntryIter_Included

#include <vector>
#include "fennel/segment/SegPageIter.h"
#include "fennel/segment/SegPageEntryIterSource.h"
#include "fennel/common/CircularBuffer.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegPageEntryIter extends SegPageIter.  Rather than pre-fetching successor
 * pages, it takes a source parameter that provides a callback method that's
 * used to determine which pages should be pre-fetched.  The class is a
 * template class, which allows context-specific information to be associated
 * with each pre-fetched page.
 */
template <class EntryT>
class SegPageEntryIter : public SegPageIter
{
    /**
     * Circular vector with the context-specific information associated with
     * each pre-fetched page.  The number of entries in this vector is not
     * necessarily the same as the number of pages that have been pre-fetched,
     * as the same page can be associated with multiple entries.
     */
    CircularBuffer<std::pair<PageId, EntryT> > entryQueue;

    /**
     * Pointer to the source that provides the callback method that
     * determines which pages to pre-fetch
     */
    SegPageEntryIterSource<EntryT> *pPrefetchSource;

    /**
     * Initializes object.
     */
    void init();

    /**
     * Determines a set of pages that should be pre-fetched and pre-fetches
     * them.
     *
     * @param prevPageId the id of the last page pre-fetched; only needed in
     * the case where the entry queue is currently empty
     *
     * @param oneIter if true, pre-fetch no more than one page
     */
    void prefetchPages(PageId prevPageId, bool oneIter);

public:
    /**
     * Constructor:  iterator starts out singular.  Initializes the object
     * with no storage yet for pre-fetch entries.
     */
    explicit SegPageEntryIter<EntryT>();

    /**
     * Constructor:  iterator starts out singular.
     *
     * @param nEntriesInit number of pre-fetch entries
     */
    explicit SegPageEntryIter<EntryT>(uint nEntriesInit);

    /**
     * Resizes the object to allow for storage of N pre-fetch entries.
     *
     * @param nEntries the number of storage slots
     */
    void resize(uint nEntries);

    /**
     * Sets the pre-fetch source for this iterator
     *
     * @param prefetchSource the source
     */
    void setPrefetchSource(SegPageEntryIterSource<EntryT> &prefetchSource);

    /**
     * Begins a new iteration.
     *
     * @param segmentAccessor accessor for the segment containing the pages to
     * visit
     *
     * @param beginPageId the ID of the first page to visit; always set to
     * NULL_PAGE_ID for this class, as the initial page is provided by the
     * source parameter
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
     * @return the current (PageId, EntryT) in the iteration
     */
    std::pair<PageId, EntryT> & operator * ()
    {
        assert(!isSingular());
        std::pair<PageId, EntryT> &entryPair = entryQueue.reference_front();
        assert(entryPair.first == prefetchQueue[iFetch]);
        return entryPair;
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
        SegPageIter::makeSingular();
        pPrefetchSource = NULL;
    }

    /**
     * @return true iff no iteration is in progress
     */
    bool isSingular() const
    {
        return (pPrefetchSource == NULL || SegPageIter::isSingular());
    }
};

FENNEL_END_NAMESPACE

#endif

// End SegPageEntryIter.h
