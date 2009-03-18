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

#ifndef Fennel_BTreeDescriptor_Included
#define Fennel_BTreeDescriptor_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BTreeDescriptor defines the information required for accessing a BTree.
 */
struct BTreeDescriptor
{
    /**
     * Accessor for segment storing BTree.
     */
    SegmentAccessor segmentAccessor;

    /**
     * Descriptor for leaf tuples.
     */
    TupleDescriptor tupleDescriptor;

    /**
     * Projection from tupleDescriptor to key.
     */
    TupleProjection keyProjection;

    /**
     * PageOwnerId used to mark pages.  Defaults to ANON_PAGE_OWNER_ID.
     */
    PageOwnerId pageOwnerId;

    /**
     * PageId of the root node, which never changes.  Set to NULL_PAGE_ID for a
     * new tree.
     */
    PageId rootPageId;

    /**
     * Optional Id of segment containing BTree data.
     */
    SegmentId segmentId;

    explicit BTreeDescriptor()
    {
        pageOwnerId = ANON_PAGE_OWNER_ID;
        rootPageId = NULL_PAGE_ID;
        segmentId = SegmentId(0);
    }
};

FENNEL_END_NAMESPACE

#endif

// End BTreeDescriptor.h
