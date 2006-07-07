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

using namespace std;

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
     * For join,
     * index 0 refers to the orginal(optimizer chosen) probe side
     * index 1 refers to the orginal(optimizer chosen) build side
     * For aggregation,
     * index 0 refers to the input.
     */
    vector<TupleDescriptor> inputDesc;

    vector<TupleProjection> keyProj;
    /*
     * If a key column is varchar type.
     */
    vector< vector<bool> > isKeyColVarChar;

    /**
     * Projections of aggs and data fields out of the RHS.
     */
    TupleProjection aggsProj;
    TupleProjection dataProj;
    
    /**
     * Build key cardinality estimate from the optimizer.
     * Used to estimate the size of the hash table(to build partial aggregates)
     * during recursive partitioning for aggregate operations.
     */
    uint cndKeys;

    /**
     * ExecStream buf accessors.
     */
    vector<SharedExecStreamBufAccessor> streamBufAccessor;

    /**
     * Special hash table properties:
     *
     * filterNull: do not add null keys to hash table
     * in join sementics: nulls do not match; however, in set matching
     * sementics, nulls are considered equal.
     *
     * removeDuplicate: do not add duplicatekeys to hash table
     * note: removeDuplicate is used only in set matching joins where
     * all inputDesc and keyProj have the same size.
     */
    bool filterNull;
    bool removeDuplicate; 
};

FENNEL_END_NAMESPACE

#endif

// End LhxHashBase.h
