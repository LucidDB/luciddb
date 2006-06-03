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

#ifndef Fennel_LhxAggExecStream_Included
#define Fennel_LhxAggExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/AggInvocation.h"
#include "fennel/exec/AggComputer.h"
#include "fennel/exec/SortedAggExecStream.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/lucidera/hashexe/LhxHashBase.h"
#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include "fennel/lucidera/hashexe/LhxPartition.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SortedAggExecStreamParams defines parameters for SortedAggExecStream.
 */
struct LhxAggExecStreamParams : public SortedAggExecStreamParams
{
    /**
     * Segment to use for storing partition pages.
     */
    SharedSegment pTempSegment;
    /**
     * Initial stats provided by the optimizer for resource allocation.
     * cndGroupByKeys: optimizer estimated cardinality of the groupby key
     */
    uint cndGroupByKeys;

    /**
     * numRows: number of input rows.
     */
    uint numRows;
};

/**
 * LhxAggExecStream aggregates its input, producing tuples of aggregate
 * function computations as output. The aggregatin is performed by using a hash
 * table.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LhxAggExecStream : public ConduitExecStream
{
    enum LhxAggState {
        Build, GetNextPlan, Partition, Produce, ProducePending,
        CreateChildPlan, Done
    };
    /**
     * Hash join info.
     */
    LhxHashInfo hashInfo;

    /**
     * Input tuple.
     */
    TupleData inputTuple;

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

    /**
     * State of the AggExecStream
     */
    LhxAggState aggState;

    /**
     * The next state of the AggExecStream
     */
    LhxAggState nextState;
  
    bool isTopPartition;
    SharedLhxPlan rootPlan;
    LhxPlan *curPlan;

    /*
     * Some temporary variables.
     */
    uint groupByKeyCount;

    AggComputerList aggComputers;
    AggComputerList partialAggComputers;

    /**
     * The build partition(which is also the only partition)
     */
    SharedLhxPartition buildPart;

    /**
     * Partition reader
     */
    LhxPartitionReader buildReader;

    /**
     * Number of tuples produced within the current quantum.
     */
    uint numTuplesProduced;

    /**
     * Index of build input(should be 0 for agg)
     */
    uint buildInputIndex;
  
    /**
     * Temporary variable used in recursive partitioning.
     *
     */
    LhxPartitionInfo partInfo;

    /**
     * implement ExecStream
     */
    virtual void closeImpl();

    /**
     * Change oroginal agg computers to compute based on partial
     * aggregates.
     */
    void getPartialAggComputers(
        AggComputerList &aggComputers,
        AggInvocationList const &aggInvocations,        
        TupleDescriptor const &keyDesc,
        TupleProjection const &aggsProj);
        
public:
    /*
     * implement ExecStream
     */
    virtual void prepare(LhxAggExecStreamParams const &params);
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

// End LhxAggExecStream.h
