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
#include "fennel/lucidera/hashexe/LhxHashBase.h"
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
     * Join keys from the left input.
     */
    TupleProjection leftKeyProj;

    /**
     * Join keys from the right input.
     */
    TupleProjection rightKeyProj;
  
    /**
     * Projection from the join. If empty then produce all input
     * columns from both join sides.
     */
    TupleProjection outputProj;

    /**
     * These two fields combined denote the join semantics
     * setDistinct setAll   JoinSementics
     *  false      false    regular join
     *  false      true     setop ALL (not implemented)
     *  true       false    setop DISTINCT
     *  true       true     invalid combination
     */
    bool setopDistinct;
    bool setopAll;

    /**
     * Initial stats provided by the optimizer for resource allocation.
     * cndKeys: key cardinality of the initial built input chosen by the
     * optimizer.
     */
    uint cndKeys;

    /**
     * numRows: number of rows of the initial built input.
     */
    uint numRows;

    /**
     * The following are testing parameters.
     */

    /**
     * Force partitioning level. Only set in tests.
     */
    uint forcePartitionLevel;

    /**
     * Whether to use join filters.
     */
    bool enableJoinFilter;

    /**
     * whether to use sub partition stats.
     */
    bool enableSubPartStat;
};

class LhxJoinExecStream : public ConfluenceExecStream
{
    enum LhxJoinInputIndex {
        LeftInputIndex=0, RightInputIndex=1
    };

    enum LhxJoinState {
        Build, GetNextPlan, Partition, Probe, CreateChildPlan, ProduceInner,
        ProduceLeftOuter, ProduceRightOuter, ProduceRightAnti, ProduceLeftSemi,
        ProducePending, ForcePartitionBuild, Done
    };
  
    /**
     * Hash join info.
     */
    LhxHashInfo hashInfo;

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
     * Number of cache blocks set aside for I/O.
     */
    uint numMiscCacheBlocks;

    /*
     * State of the JoinExecStream
     */
    LhxJoinState joinState;

    /**
     * The next state of the JoinExecStream
     */
    LhxJoinState nextState;

    /*
     * Join semantics
     */

    /**
     * Return non-matching rows from the left.
     */
    bool leftOuter;

    /**
     * Return non-matching rows from the right.
     */
    bool rightOuter;

    /**
     * Inner join: only return matching rows.
     */
    bool innerJoin;

    /**
     * Return non-matching rows from both sides.
     */
    bool fullOuter;

    /**
     * Return matching rows from the left.
     */
    bool leftSemi;

    /**
     * Return non-matching rows from the right.
     */
    bool rightAnti;

    /**
     * regularJoin:   do not match NULLs,
     *                and do not remove duplicates in inputs.
     * setopDistinct: match NULLs, and remove duplicates in inputs.
     * setopAll:      match NULLs, and do not remove duplicates in inputs
     * Note: setopAll is not implemented yet.
     */
    bool regularJoin;
    bool setopDistinct;
    bool setopAll;

    /**
     * Whether this join filters null key values(when they are not already
     * filtered at the input)
     */
    bool leftFilterNull;
    bool rightFilterNull;

    /*
     * Related to set match semantics
     */
    bool removeDuplicateProbe;
    bool removeDuplicateBuild;

    /*
     * Some temporary variables.
     */

    /**
     * Number of tuples produced within the current quantum.
     */
    uint numTuplesProduced;

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
     * Force partitioning level. Only set in tests.
     */
    uint forcePartitionLevel;

    /**
     * Whether to use join filters.
     */
    bool enableJoinFilter;

    /**
     * whether to use sub partition stats.
     */
    bool enableSubPartStat;

    /**
     * implement ExecStream
     */
    virtual void closeImpl();

    void setJoinType(LhxJoinExecStreamParams const &params);

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
