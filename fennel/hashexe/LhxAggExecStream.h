/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
#include "fennel/hashexe/LhxHashBase.h"
#include "fennel/hashexe/LhxHashTable.h"
#include "fennel/hashexe/LhxPartition.h"

FENNEL_BEGIN_NAMESPACE

// REVIEW jvs 25-Aug-2006: Seems like a common base AggExecStreamParams is in
// order (with SortedAggExecStreamParams left empty for now)
/**
 * LhxAggExecStreamParams defines parameters for SortedAggExecStream.
 */
struct LhxAggExecStreamParams : public SortedAggExecStreamParams
{
    /**
     * Segment to use for storing partition pages.
     */
    SharedSegment pTempSegment;

    /**
     * Whether to use sub partition stats.
     */
    bool enableSubPartStat;

    /**
     * This parameter is used only in tests.
     * Force partitioning level.
     */
    uint forcePartitionLevel;

    // REVIEW jvs 25-Aug-2006:  When using Javadoc/doxygen comments,
    // it's important to remember that each comment will show up
    // separately in the generated class documentation.  Below,
    // the first comment says "Initial stats ...", which
    // really applies to both comments.  Instead, put a summary
    // at the class-level comment, like "The fields cndGroupByKeys and numRows
    // are provided by the optimizer to help with estimating
    // resource allocation requirements" and then on the field-level
    // comments, just say what they are, e.g. "Estimate for number of rows from
    // the build input, etc."  (No need to repeat the field name inside
    // of the field-level comments.)
    /**
     * Initial stats provided by the optimizer for resource allocation.
     * cndKeys: key cardinality of the initial built input chosen by the
     * optimizer. For Hash Aggregate, this is the estimated number of groups.
     * If < 0, stat is unknown.
     */
    RecordNum cndGroupByKeys;

    /**
     * numRows: number of rows from the build input.  If < 0, unknown.
     */
    RecordNum numRows;
};

/**
 * LhxAggExecStream aggregates its input, producing tuples of aggregate
 * function computations as output. The aggregation is performed by using a hash
 * table.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LhxAggExecStream : public ConduitExecStream
{
    // REVIEW jvs 26-Aug-2006:  Fennel convention for enum names is
    // all uppercase with underscores

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

    // REVIEW jvs 25-Aug-2006:  This member is only accessed within
    // one method (execute).  Wouldn't it be easier to make it a local
    // variable there so it doesn't have to be reset?
    /**
     * Number of tuples produced within the current quantum.
     */
    uint numTuplesProduced;

    /**
     * Hash aggregation info.
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
    BlockNum numBlocksHashTable;

    /**
     * Number of cache blocks set aside for I/O.  If MAXU, no stats are
     * available to compute this value.
     */
    BlockNum numMiscCacheBlocks;

    // REVIEW jvs 25-Aug-2006:  Next three fields need comments, maybe
    // a reference to somewhere else explaining the plan concept.  Is
    // it true that isTopPlan can be derived from (curPlan == rootPlan.get())?

    /*
     * Plan
     */
    bool isTopPlan;
    SharedLhxPlan rootPlan;

    // REVIEW jvs 25-Aug-2006: If there's a valid reason not to declare this as
    // a SharedLhxPlan (like performance, which seems justified), then that
    // reason should be explained, since in general mixing shared and
    // non-shared pointers can be error-prone.
    LhxPlan *curPlan;

    // REVIEW jvs 25-Aug-2006: This will always be 0, right?  In that case, use
    // a static const to make it obvious, and assert accordingly in ::prepare.
    // And then use BUILD_INPUT_INDEX naming convention.

    /**
     * Index of build input(should be 0 for agg)
     */
    uint buildInputIndex;

    /**
     * Partition context used in recursive partitioning.
     */
    LhxPartitionInfo partInfo;

    /**
     * The build partition (which is also the only partition)
     */
    SharedLhxPartition buildPart;

    /**
     * Partition reader
     */
    LhxPartitionReader buildReader;

    /**
     * Whether to use sub partition stats.
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

    // REVIEW jvs 25-Aug-2006: This seems fairly useless in LhxAggExecStream;
    // it only ever gets set to Produce.  I think it's vestigial from
    // LhxJoinExecStream.
    /**
     * The next state of the AggExecStream
     */
    LhxAggState nextState;


    // REVIEW jvs 25-Aug-2006: This is so temporary that it is never
    // even referenced anywhere?
    /*
     * Some temporary variables.
     */
    uint groupByKeyCount;

    // REVIEW jvs 25-Aug-2006:  Next two fields need comments, maybe
    // a reference to somewhere else explaining the concept of partial
    // aggregation.

    AggComputerList aggComputers;
    AggComputerList partialAggComputers;

    // implement ExecStream
    virtual void closeImpl();

    /*
     * Set up hashInfo from exec stream parameters.
     */
    void setHashInfo(LhxAggExecStreamParams const &params);

    /*
     * Set up the aggregate computers and partial aggregate computers used by
     * the hash table.
     */
    void setAggComputers(
        LhxHashInfo &hashInfo,
        AggInvocationList const &aggInvocations);

public:
    // implement ExecStream
    virtual void prepare(LhxAggExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);

    virtual void setResourceAllocation(
        ExecStreamResourceQuantity &quantity);

};

FENNEL_END_NAMESPACE

#endif

// End LhxAggExecStream.h
