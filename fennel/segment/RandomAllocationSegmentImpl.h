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

#ifndef Fennel_RandomAllocationSegmentImpl_Included
#define Fennel_RandomAllocationSegmentImpl_Included

#include "fennel/segment/RandomAllocationSegment.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// the structs below

/**
 * ExtentAllocationNode is the allocation map for one extent
 * in a RandomAllocationSegment.
 */
struct ExtentAllocationNode : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xb9ca99dced182239LL;

    PageEntry &getPageEntry(uint i)
    {
        return reinterpret_cast<PageEntry *>(this+1)[i];
    }

    PageEntry const &getPageEntry(uint i) const
    {
        return reinterpret_cast<PageEntry const *>(this+1)[i];
    }
};

typedef SegNodeLock<ExtentAllocationNode> ExtentAllocLock;


FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegmentImpl.h
