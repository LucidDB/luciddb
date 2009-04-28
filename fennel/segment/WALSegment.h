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

#ifndef Fennel_WALSegment_Included
#define Fennel_WALSegment_Included

#include "fennel/segment/DelegatingSegment.h"
#include "fennel/synch/SynchObj.h"

FENNEL_BEGIN_NAMESPACE

/**
 * WALSegment is an implementation of Segment which keeps track of pages as
 * they are dirtied and flushed.  This information can be used to implement the
 * write-ahead log (WAL) protocol.  See <a
 * href="structSegmentDesign.html#WALSegment">the design docs</a> for more
 * detail.
 */
class FENNEL_SEGMENT_EXPORT WALSegment
    : public DelegatingSegment
{
    friend class SegmentFactory;

    mutable StrictMutex mutex;
    PageSet dirtyPageSet;

    explicit WALSegment(SharedSegment logSegment);

public:

    virtual ~WALSegment();

    /**
     * Determines the ID of the lowest dirty log page.
     *
     * @return the PageId, or NULL_PAGE_ID if no dirty log pages
     */
    PageId getMinDirtyPageId() const;

    // implement the MappedPageListener interface
    virtual void notifyPageDirty(CachePage &page,bool bDataValid);
    virtual void notifyAfterPageFlush(CachePage &page);
    virtual void notifyPageUnmap(CachePage &page);
};

FENNEL_END_NAMESPACE

#endif

// End WALSegment.h
