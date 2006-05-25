/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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

#ifndef Fennel_LhxHashBase_Included
#define Fennel_LhxHashBase_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Information common to all hash execution components: join, aggregate.
 *
 * @author Rushan Chen
 * @version $Id$
 */
struct LhxHashInfo
{
    /**
     * Note: need two accessors: one for writing to memory, the other one for
     * writing to the disk. Probably need a list of the second kind, each using
     * one cache page for I/O. See ExternalSorExecStream for models of
     * initializing and using these two accessors.
     */
    /**
     * Accessor for segment used to store runs externally.
     */
    SegmentAccessor externalSegmentAccessor;

    /**
     * Accessor for scratch segment used for building runs in-memory.
     */
    SegmentAccessor memSegmentAccessor;

    /**
     * Cache pages to use by this join. These pages are used for 
     * (1) building hash table
     * (2) buffering I/O for writing out to partitions on disk.
     */
    uint numCachePages;

    /**
     * Join keys, aggs and data
     * index 0 refers to the orginal(optimizer chosen) probe side
     * index 1 refers to the orginal(optimizer chosen) build side
     */
    std::vector<TupleDescriptor> inputDesc;

    std::vector<TupleProjection> keyProj;
    /*
     * If a key column is varchar type.
     */
    std::vector< std::vector<bool> > isKeyColVarChar;

    /**
     * Projections of aggs and data fields out of the RHS.
     */
    TupleProjection aggsProj;
    TupleProjection dataProj;

    /**
     * ExecStream buf accessors.
     */
    std::vector<SharedExecStreamBufAccessor> streamBufAccessor;
};

FENNEL_END_NAMESPACE

#endif

// End LhxHashBase.h
