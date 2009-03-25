/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2004-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_ExternalSortInfo_Included
#define Fennel_ExternalSortInfo_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentAccessor.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * Global information shared with sorter sub-components.
 */
struct ExternalSortInfo
{
    /**
     * Main stream, for abort checking.  Note that we intentionally
     * don't give subcomponents access to ExternalSortExecStreamImpl
     * details.
     */
    ExecStream &stream;

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
     * @see ExternalSortExecStreamParams
     */
    std::vector<bool> descendingKeyColumns;

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

    explicit ExternalSortInfo(ExecStream &);

    /**
     * Compares two keys, taking ASC/DESC into account.
     *
     * @param key1 first key to compare
     *
     * @param key2 second key to compare
     *
     * @return negative for key1 < key2; zero for key1 == key2;
     * positive for key1 > key2
     */
    int compareKeys(TupleData const &key1, TupleData const &key2);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortInfo.h
