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
#include "fennel/exec/SortedAggExecStream.h"
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
     * whether to use sub partition stats.
     */
    bool enableSubPartStat;

    /**
     * This parameter is used only in tests.
     * Force partitioning level.
     */
    uint forcePartitionLevel;

    /**
     * Initial stats provided by the optimizer for resource allocation.
     * cndKeys: key cardinality of the initial built input chosen by the
     * optimizer. For Hash Aggregate, this is the estimated number of groups.
     */
    uint cndGroupByKeys;

    /**
     * numRows: number of rows from the build input.
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
        ForcePartitionBuild, Build, Produce, ProducePending,
        Partition, CreateChildPlan, GetNextPlan, Done
    };

    /**
     * Input tuple.
     */
    TupleData inputTuple;

    /**
     * TupleData to assemble the output tuple.
     */
    TupleData outputTuple;

    /**
     * Number of tuples produced within the current quantum.
     */
    uint numTuplesProduced;


    /**
     * Hash join info.
     */
    LhxHashInfo hashInfo;

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
     * Plan
     */
    bool isTopPlan;
    SharedLhxPlan rootPlan;
    LhxPlan *curPlan;

    /**
     * Index of build input(should be 0 for agg)
     */
    uint buildInputIndex;
  
    /**
     * Partition context used in recursive partitioning.
     *
     */
    LhxPartitionInfo partInfo;
    /**
     * The build partition(which is also the only partition)
     */
    SharedLhxPartition buildPart;

    /**
     * Partition reader
     */
    LhxPartitionReader buildReader;

    /**
     * whether to use sub partition stats.
     */
    bool enableSubPartStat;

    /**
     * This is set only in tests.
     * Force partitioning level.
     */
    uint forcePartitionLevel;

    /**
     * State of the AggExecStream
     */
    LhxAggState aggState;

    /**
     * The next state of the AggExecStream
     */
    LhxAggState nextState;
  

    /*
     * Some temporary variables.
     */
    uint groupByKeyCount;

    AggComputerList aggComputers;
    AggComputerList partialAggComputers;

    /**
     * implement ExecStream
     */
    virtual void closeImpl();

    /*
     * Set up hashInfo from exec stream parameters.
     */
    void setHashInfo(LhxAggExecStreamParams const &params);

    /*
     * set up the aggregate computers and partial aggregate computers used by
     * the hash table.
     */
    void setAggComputers(
        LhxHashInfo &hashInfo,
        AggInvocationList const &aggInvocations);

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
