/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/LinearViewSegment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LinearViewSegment::LinearViewSegment(
    SharedSegment delegateSegment,
    PageId firstPageId)
    : DelegatingSegment(delegateSegment)
{
    while (firstPageId != NULL_PAGE_ID) {
        // TODO:  something more efficient
        pageTable.push_back(firstPageId);
        firstPageId = DelegatingSegment::getPageSuccessor(firstPageId);
    }
}

LinearViewSegment::~LinearViewSegment()
{
}

BlockId LinearViewSegment::translatePageId(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    BlockNum blockNum = getLinearBlockNum(pageId);
    PageId underlyingPageId = pageTable[blockNum];
    return DelegatingSegment::translatePageId(underlyingPageId);
}

PageId LinearViewSegment::translateBlockId(BlockId blockId)
{
    assert(!pageTable.empty());
    PageId underlyingPageId =
        DelegatingSegment::translateBlockId(blockId);
    std::vector<PageId>::const_iterator pFound;
    pFound = std::find(pageTable.begin(),pageTable.end(),underlyingPageId);
    assert(pFound != pageTable.end());
    return getLinearPageId(pFound - pageTable.begin());
}

BlockNum LinearViewSegment::getAllocatedSizeInPages()
{
    return pageTable.size();
}

PageId LinearViewSegment::allocatePageId(PageOwnerId ownerId)
{
    PageId underlyingPageId = DelegatingSegment::allocatePageId(ownerId);
    if (underlyingPageId == NULL_PAGE_ID) {
        return underlyingPageId;
    }
    if (!pageTable.empty()) {
        DelegatingSegment::setPageSuccessor(
            pageTable.back(),underlyingPageId);
    }
    PageId pageId = getLinearPageId(pageTable.size());
    pageTable.push_back(underlyingPageId);
    return pageId;
}

void LinearViewSegment::deallocatePageRange(PageId startPageId,PageId endPageId)
{
    // TODO:  support truncation with startPageId != NULL_PAGE_ID
    assert(startPageId == NULL_PAGE_ID);
    assert(endPageId == NULL_PAGE_ID);
    pageTable.clear();
}

bool LinearViewSegment::isPageIdAllocated(PageId pageId)
{
    return isLinearPageIdAllocated(pageId);
}

PageId LinearViewSegment::getFirstPageId() const
{
    if (pageTable.empty()) {
        return NULL_PAGE_ID;
    } else {
        return pageTable.front();
    }
}

PageId LinearViewSegment::getPageSuccessor(PageId pageId)
{
    return getLinearPageSuccessor(pageId);
}

void LinearViewSegment::setPageSuccessor(PageId pageId,PageId successorId)
{
    setLinearPageSuccessor(pageId,successorId);
}

Segment::AllocationOrder LinearViewSegment::getAllocationOrder() const
{
    return LINEAR_ALLOCATION;
}

PageId LinearViewSegment::updatePage(PageId pageId, bool needsTranslation)
{
    assert(isPageIdAllocated(pageId));
    BlockNum blockNum = getLinearBlockNum(pageId);
    PageId underlyingPageId = pageTable[blockNum];
    return DelegatingSegment::updatePage(underlyingPageId, needsTranslation);
}

FENNEL_END_CPPFILE("$Id: //open/dt/dev/fennel/segment/LinearViewSegment.cpp#6 $");

// End LinearViewSegment.cpp
