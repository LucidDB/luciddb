/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Red Square, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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

#ifndef Fennel_ExternalSortInfo_Included
#define Fennel_ExternalSortInfo_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Global information shared with sorter sub-components.
 */
struct ExternalSortInfo
{
    /**
     * Accessor for segment used to store runs externally.
     */
    SegmentAccessor externalSegmentAccessor;

    /**
     * Accessor for scratch segment used for building runs in-memory.
     */
    SegmentAccessor memSegmentAccessor;

    /**
     * Descriptor for tuples to be sorted.
     */
    TupleDescriptor tupleDesc;

    /**
     * Projection of sort keys from tupleDesc.
     */
    TupleProjection keyProj;

    /**
     * Descriptor for projected sort key tuples.
     */
    TupleDescriptor keyDesc;

    /**
     * Maximum number of memory pages available for sorting.
     */
    uint nSortMemPages;

    /**
     * Maximum number of memory pages which can be filled per run.
     */
    uint nSortMemPagesPerRun;

    /**
     * Number of bytes per memory page.  Must be a power of 2.
     */
    uint cbPage;

    explicit ExternalSortInfo();
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortInfo.h
