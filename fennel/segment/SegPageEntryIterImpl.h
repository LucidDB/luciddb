/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_SegPageEntryIterImpl_Included
#define Fennel_SegPageEntryIterImpl_Included

#include "fennel/segment/SegPageEntryIter.h"
#include "fennel/segment/SegPageEntryIterSource.h"

FENNEL_BEGIN_NAMESPACE

template <class EntryT>
SegPageEntryIter<EntryT>::SegPageEntryIter()
{
    init();
}

template <class EntryT>
SegPageEntryIter<EntryT>::SegPageEntryIter(uint nEntriesInit)
    : entryQueue(nEntriesInit)
{
    init();
}

template <class EntryT>
void SegPageEntryIter<EntryT>::init()
{
    pPrefetchSource = NULL;
}

template <class EntryT>
void SegPageEntryIter<EntryT>::resize(uint nEntries)
{
    entryQueue.resize(nEntries);
}

template <class EntryT>
void SegPageEntryIter<EntryT>::setPrefetchSource(
    SegPageEntryIterSource<EntryT> &prefetchSource)
{
    assert(entryQueue.size() > 0);
    pPrefetchSource = &prefetchSource;
    for (uint i = 0; i < entryQueue.size(); i++) {
        pPrefetchSource->initPrefetchEntry(entryQueue[i].second);
    }
}

template <class EntryT>
void SegPageEntryIter<EntryT>::mapRange(
    SegmentAccessor const &segmentAccessorInit,
    PageId beginPageIdInit,
    PageId endPageIdInit)
{
    assert(segmentAccessorInit.pSegment);
    assert(segmentAccessorInit.pCacheAccessor);
    assert(beginPageIdInit == NULL_PAGE_ID);
    segmentAccessor = segmentAccessorInit;
    endPageId = endPageIdInit;
    initPrefetchQueue();
    entryQueue.clear();

    // Pre-populate the queues
    prefetchPages(NULL_PAGE_ID, false);
}

template <class EntryT>
void SegPageEntryIter<EntryT>::prefetchPages(PageId prevPageId, bool oneIter)
{
    if (!entryQueue.empty()) {
        prevPageId = entryQueue.reference_back().first;
    }

    // Continue retrieving pageIds while we have available space in
    // the prefetch queue
    while (nFreePageSlots != 0 || oneIter) {
        // But make sure we have space in the entryQueue
        if (!entryQueue.spaceAvailable()) {
            break;
        }

        std::pair<PageId, EntryT> &entryPair =
            entryQueue[entryQueue.getLastPos() + 1];
        bool found;
        entryPair.first =
            pPrefetchSource->getNextPageForPrefetch(entryPair.second, found);
        if (!found) {
            break;
        }
        entryQueue.push_back(entryPair);

        if (entryPair.first == endPageId) {
            atEnd = 1;
        }

        if (atEnd || entryPair.first != prevPageId) {
            prefetchPage(entryPair.first);
        }

        if (atEnd || oneIter) {
            break;
        }

        prevPageId = entryPair.first;
    }
}

template <class EntryT>
void SegPageEntryIter<EntryT>::operator ++ ()
{
    assert(!isSingular());
    std::pair<PageId, EntryT> &entryPair = entryQueue.reference_front();
    PageId currPageId = entryPair.first;
    assert(currPageId != endPageId);

    entryQueue.pop_front();

    // If the queue is empty, we have to try and pre-fetch some more entries.
    // Worst case, we'll hit the ending page.  The queueSize == 1 case needs
    // special handling because:
    // 1) There are currently no free slots in the prefetch queue.  The one
    //    slot is occupied by the last page pre-fetched.
    // 2) We can only replace that slot with a new page if it's different
    //    from that last pre-fetched page.
    // Therefore, we can't assume that the current page will always be
    // replaced by a new one.
    if (entryQueue.empty()) {
        prefetchPages(currPageId, (queueSize == 1));
    }

    PageId nextPageId = entryQueue.reference_front().first;
    // Bump up the prefetch queue only if we've moved on to a new page
    if (nextPageId != currPageId) {
        iFetch = (iFetch + 1) % queueSize;
        nFreePageSlots++;
    }

    // Re-populate the queues if there's available space
    if (!atEnd) {
        prefetchPages(NULL_PAGE_ID, false);
    }
}

FENNEL_END_NAMESPACE

#endif

// End SegPageEntryIterImpl.h
