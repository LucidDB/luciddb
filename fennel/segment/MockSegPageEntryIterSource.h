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

#ifndef Fennel_MockSegPageEntryIterSource_Included
#define Fennel_MockSegPageEntryIterSource_Included

#include "fennel/segment/SegPageEntryIterSource.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

class Segment;

/**
 * A mock SegPageEntryIterSource that pre-fetches every other page, returning
 * each page twice.  The context associated with each page returned is a
 * sequencing counter, starting at 0.
 */
class MockSegPageEntryIterSource : public SegPageEntryIterSource<int>
{
    int counter;
    PageId nextPageId;
    SegmentAccessor segmentAccessor;

public:
    explicit MockSegPageEntryIterSource(
        SegmentAccessor const &segmentAccessorInit,
        PageId beginPageId);
    virtual ~MockSegPageEntryIterSource();
    virtual PageId getNextPageForPrefetch(int &entry, bool &found);
};

FENNEL_END_NAMESPACE

#endif

// End MockSegPageEntryIterSource.h
