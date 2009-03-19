/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
#include "fennel/segment/SegStreamAllocation.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegStreamAllocation SegStreamAllocation::newSegStreamAllocation()
{
    return SharedSegStreamAllocation(
        new SegStreamAllocation(),
        ClosableObjectDestructor());
}

SegStreamAllocation::SegStreamAllocation()
{
    nPagesWritten = 0;
}

void SegStreamAllocation::beginWrite(SharedSegOutputStream pSegOutputStreamInit)
{
    // go from state UNALLOCATED
    assert(!pSegOutputStream);
    assert(!pSegInputStream);

    // to state WRITING
    needsClose = true;
    pSegOutputStream = pSegOutputStreamInit;
}

void SegStreamAllocation::endWrite()
{
    // from state WRITING
    assert(pSegOutputStream);

    nPagesWritten = pSegOutputStream->getPageCount();

    SegmentAccessor segmentAccessor = pSegOutputStream->getSegmentAccessor();
    PageId firstPageId = pSegOutputStream->getFirstPageId();
    pSegOutputStream->close();
    pSegOutputStream.reset();
    if (firstPageId == NULL_PAGE_ID) {
        // go directly to UNALLOCATED
        return;
    }

    // go to state READING
    pSegInputStream = SegInputStream::newSegInputStream(
        segmentAccessor,
        firstPageId);
    pSegInputStream->setDeallocate(true);
}

SharedSegInputStream const &SegStreamAllocation::getInputStream() const
{
    return pSegInputStream;
}

void SegStreamAllocation::closeImpl()
{
    if (pSegOutputStream) {
        // state WRITING
        assert(!pSegInputStream);
        // do a fake read; this won't really read the pages, it will just
        // deallocate them
        endWrite();
    }

    if (pSegInputStream) {
        // state READING
        assert(!pSegOutputStream);
        assert(pSegInputStream->isDeallocating());
        // this will deallocate all remaining pages as a side-effect
        pSegInputStream->close();
        pSegInputStream.reset();
    } else {
        // state UNALLOCATED:  nothing to do
    }

    nPagesWritten = 0;
}

BlockNum SegStreamAllocation::getWrittenPageCount() const
{
    if (pSegOutputStream) {
        return pSegOutputStream->getPageCount();
    } else {
        return nPagesWritten;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End SegStreamAllocation.cpp
