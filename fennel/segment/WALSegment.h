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

#ifndef Fennel_WALSegment_Included
#define Fennel_WALSegment_Included

#include "fennel/segment/DelegatingSegment.h"
#include "fennel/synch/SynchObj.h"

#include <set>

FENNEL_BEGIN_NAMESPACE

/**
 * WALSegment is an implementation of Segment which keeps track of pages as
 * they are dirtied and flushed.  This information can be used to implement the
 * write-ahead log (WAL) protocol.  See <a
 * href="structSegmentDesign.html#WALSegment">the design docs</a> for more
 * detail.
 */
class WALSegment : public DelegatingSegment
{
    friend class SegmentFactory;
    
    typedef std::set<PageId> PageSet;
    typedef PageSet::const_iterator PageSetConstIter;
    
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
