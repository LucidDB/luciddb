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
#include "fennel/segment/WALSegment.h"
#include "fennel/cache/CachePage.h"

FENNEL_BEGIN_CPPFILE("$Id$");

WALSegment::WALSegment(SharedSegment logSegment)
    : DelegatingSegment(logSegment)
{
    assert(
        DelegatingSegment::getAllocationOrder() >=
        Segment::ASCENDING_ALLOCATION);
}

WALSegment::~WALSegment()
{
    assert(dirtyPageSet.empty());
}

void WALSegment::notifyPageDirty(CachePage &page,bool bDataValid)
{
    DelegatingSegment::notifyPageDirty(page,bDataValid);
    PageId logPageId = translateBlockId(
        page.getBlockId());
    StrictMutexGuard mutexGuard(mutex);
    dirtyPageSet.insert(dirtyPageSet.end(),logPageId);
}

void WALSegment::notifyAfterPageFlush(CachePage &page)
{
    DelegatingSegment::notifyAfterPageFlush(page);
    PageId logPageId = translateBlockId(page.getBlockId());
    StrictMutexGuard mutexGuard(mutex);
    dirtyPageSet.erase(logPageId);
}

void WALSegment::notifyPageUnmap(CachePage &page)
{
    if (!page.isDirty()) {
        return;
    }
    notifyAfterPageFlush(page);
}

PageId WALSegment::getMinDirtyPageId() const
{
    StrictMutexGuard mutexGuard(mutex);
    if (dirtyPageSet.empty()) {
        return NULL_PAGE_ID;
    }
    PageId minDirtyPageId = *(dirtyPageSet.begin());
    return minDirtyPageId;
}

FENNEL_END_CPPFILE("$Id$");

// End WALSegment.cpp
