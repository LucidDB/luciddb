/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_LinearViewSegment_Included
#define Fennel_LinearViewSegment_Included

#include "fennel/segment/DelegatingSegment.h"
#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * LinearViewSegment is an implementation of Segment in terms of an underlying
 * Segment, which must support the get/setPageSuccessor interface.  See <a
 * href="structSegmentDesign.html#LinearViewSegment">the design docs</a> for
 * more detail.
 *
 *<p>
 *
 * Deallocation of individual pages of a LinearViewSegment is not yet supported,
 * but full truncation is.
 *
 */
class LinearViewSegment : public DelegatingSegment
{
    friend class SegmentFactory;

    std::vector<PageId> pageTable;
    
    LinearViewSegment(
        SharedSegment delegateSegment,
        PageId firstPageId);

public:
    virtual ~LinearViewSegment();
    
    /**
     * @return the starting PageId in the underlying segment
     */
    PageId getFirstPageId() const;
    
    // implementation of Segment interface
    
    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual void deallocatePageRange(PageId startPageId,PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual BlockNum getAllocatedSizeInPages();
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId,PageId successorId);
    virtual AllocationOrder getAllocationOrder() const;
};

FENNEL_END_NAMESPACE

#endif

// End LinearViewSegment.h
