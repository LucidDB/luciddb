/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_SegPageEntryIterSource_Included
#define Fennel_SegPageEntryIterSource_Included

FENNEL_BEGIN_NAMESPACE

/**
 * SegPageEntryIterSource provides the callback method that supplies pre-fetch
 * pageIds for SegPageEntryIter.
 */
template <class EntryT>
class SegPageEntryIterSource
{
public:
    virtual ~SegPageEntryIterSource()
    {
    }

    /**
     * Initializes a specific entry in the pre-fetch queue.
     *
     * @param entry the entry that will be initialized
     */
    virtual void initPrefetchEntry(EntryT &entry)
    {
    }

    /**
     * Retrieves the next pageId to be pre-fetched, also filling in context-
     * specific information associated with the page.
     *
     * @param [in, out] entry the context-specific information to be filled in
     *
     * @param [out] found true if a pageId has been found and should be added
     * to the pre-fetch queue
     *
     * @return the prefetch pageId
     */
    virtual PageId getNextPageForPrefetch(EntryT &entry, bool &found) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End SegPageEntryIterSource.h
