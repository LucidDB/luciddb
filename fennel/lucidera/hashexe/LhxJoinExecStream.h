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

#ifndef Fennel_LhxJoinExecStream_Included
#define Fennel_LhxJoinExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/lucidera/hashexe/LhxHashTable.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LhxJoinExecStream matches two input streams by using a hash table built from
 * one of the inputs(usually the smaller input).
 *
 * @author Rushan Chen
 * @version $Id$
 */
struct LhxJoinExecStreamParams : public ConfluenceExecStreamParams
{
    /**
     * Return matching rows from the left.
     */
    bool leftInner;

    /**
     * Return non-matching rows from the left.
     */
    bool leftOuter;

    /**
     * Return matching rows from the right.
     */
    bool rightInner;

    /**
     * Return non-matching rows from the right.
     */
    bool rightOuter;

    /**
     * Used for both hash aggregate and hash distinct(which has only one group,
     * and no aggregate calculators)
     */
    bool eliminateDuplicate;

    /**
     * Join keys from the left input.
     */
    TupleProjection leftKeyProj;

    /**
     * Join keys from the right input.
     */
    TupleProjection rightKeyProj;
  
    /**
     * Projection from the cartesian product. If empty then produce all input
     * columns from both join sides.
     */
    TupleProjection outputProj;

    /**
     * Initial stats provided by the optimizer for resource allocation.
     * cndKeys: key cardinality of the initial built input(chosen by the
     * optimizer).
     */
    uint cndKeys;

    /**
     * numRows: number of rows of the initial built input.
     */
    uint numRows;

    /*
     * TODO: information about aggregates here.
     */
    uint aggsCount;
};

struct LhxJoinInfo
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

};

enum JoinState {
    Building, Probing, ProducingInner, ProducingLeftOuter, ProducingRightOuter,
    ProducePending, Done
};


class LhxJoinExecStream : public ConfluenceExecStream
{
  
    /**
     * Hash join info.
     */
    LhxJoinInfo joinInfo;

    /**
     * Join keys, aggs and data
     */
    TupleProjection leftKeyProj;
    TupleProjection rightKeyProj;

    /**
     * Projections of aggs and data fields out of the RHS.
     */
    TupleProjection aggsProj;
    TupleProjection dataProj;

    /**
     * Input tuple.
     */
    TupleData leftTuple;
    TupleData rightTuple;

    /**
     * TupleData to assemble the output tuple.
     */
    TupleData outputTuple;

    /**
     * HashTable to use.
     */
    LhxHashTable hashTable;
    LhxHashTableReader hashTableReader;

    /**
     * Initial estimate of blocks required.
     */
    uint numBlocksHashTable;

    /**
     * Initial estimate of slots required.
     */
    uint numSlotsHashTable;

    /*
     * State of the JoinExecStream
     */
    JoinState joinState;

    /**
     * Return matching rows from the left.
     */
    bool leftInner;

    /**
     * Return non-matching rows from the left.
     */
    bool leftOuter;

    /**
     * Return matching rows from the right.
     */
    bool rightInner;

    /**
     * Return non-matching rows from the right.
     */
    bool rightOuter;

    /*
     * Should this join filter null key values(when they are not already
     * filtered at the input)
     */
    bool leftFilterNull;
    bool rightFilterNull;
    
    /*
     * Number of tuples produced within the current quantum.
     */
    uint numTuplesProduced;

    /**
     * implement ExecStream
     */
    virtual void closeImpl();
        
public:
    /*
     * implement ExecStream
     */
    virtual void prepare(LhxJoinExecStreamParams const &params);

    virtual void open(bool restart);

    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);

    virtual void setResourceAllocation(
        ExecStreamResourceQuantity &quantity);
};

FENNEL_END_NAMESPACE

#endif

// End LhxJoinExecStream.h
