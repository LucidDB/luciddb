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
#include "fennel/lucidera/hashexe/LhxJoinBase.h"
#include "fennel/lucidera/hashexe/LhxPartition.h"

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
     * Segment to use for storing partition pages.
     */
    SharedSegment pTempSegment;

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

enum LhxJoinState {
    Build, GetNextPlan, Partition, Probe, ProduceInner, ProduceLeftOuter,
    ProduceRightOuter, ProducePending, CreateChildPlan, Done
};

class LhxJoinExecStream : public ConfluenceExecStream
{
  
    /**
     * Hash join info.
     */
    LhxJoinInfo joinInfo;

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
    LhxJoinState joinState;

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
     * Whether this join filters null key values(when they are not already
     * filtered at the input)
     */
    bool leftFilterNull;
    bool rightFilterNull;

    /*
     * Some temporary variables.
     */

    /**
     * Number of tuples produced within the current quantum.
     */
    uint numTuplesProduced;

    /**
     * The next state of the JoinExecStream
     */
    LhxJoinState nextState;

    /**
     * tuple size
     */
    uint leftTupleSize;
    uint rightTupleSize;

    /*
     * Temporary variables used.
     *
     */
    SharedLhxPartition leftPart;
    SharedLhxPartition rightPart;

    LhxPartitionReader leftReader;
    LhxPartitionReader rightReader;

    bool isTopPartition;
    SharedLhxPlan rootPlan;
    LhxPlan *curPlan;

    /**
     * Temporary variable used in recursive partitioning.
     *
     */
    LhxPartitionInfo partInfo;

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
