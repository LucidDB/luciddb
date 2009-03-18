/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
#include "fennel/lucidera/hashexe/LhxHashBase.h"
#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include "fennel/lucidera/hashexe/LhxPartition.h"

using namespace boost;

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
     * whether to use sub partition stats.
     */
    bool enableSubPartStat;

    /**
     * This parameter is used only in tests.
     * Force partitioning level.
     */
    uint forcePartitionLevel;

    // REVIEW jvs 25-Aug-2006: See my comments on LhxAggExecStreamParams
    // regarding these fields (w.r.t. comment cross-refs).

    /**
     * Initial stats provided by the optimizer for resource allocation.
     * cndKeys: key cardinality of the initial built input chosen by the
     * optimizer. For Hash Aggregate, this is the estimated number of groups.
     * If MAXU, the stat is unknown.
     */
    RecordNum cndKeys;

    /**
     * numRows: number of rows from the build input.  If MAXU, unknown.
     */
    RecordNum numRows;

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
     * Join keys from the right input.
     */
    TupleProjection filterNullKeyProj;

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
     * Whether to use join filters.
     */
    bool enableJoinFilter;

    /**
     * Whether to use swing based on input sizes.
     */
    bool enableSwing;
};

class LhxJoinExecStream : public ConfluenceExecStream
{
    // REVIEW jvs 26-Aug-2006:  Fennel convention for enum names is
    // all uppercase with underscores

    enum LhxDefaultJoinInputIndex {
        DefaultProbeInputIndex = 0, DefaultBuildInputIndex = 1
    };

    enum LhxJoinState {
        ForcePartitionBuild, Build, Probe,
        ProduceBuild, ProducePending,
        Partition, CreateChildPlan, GetNextPlan, Done
    };

    /**
     * Input tuple.
     */
    shared_array<TupleData> inputTuple;
    shared_array<uint> inputTupleSize;

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
     * Initial estimate of blocks required.  If MAXU, no stats are available
     * to compute this value.
     */
    BlockNum numBlocksHashTable;

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
     * Partition context used in recursive partitioning.
     *
     */
    LhxPartitionInfo partInfo;

    /**
     * The build partition(which is also the only partition)
     */
    SharedLhxPartition buildPart;
    SharedLhxPartition probePart;

    /**
     * Partition reader
     */
    LhxPartitionReader buildReader;
    LhxPartitionReader probeReader;

    /**
     * whether to use sub partition stats.
     */
    bool enableSubPartStat;

    /**
     * Whether to use swing based on input sizes.
     */
    bool enableSwing;

    /**
     * This is set only in tests.
     * Force partitioning level.
     */
    uint forcePartitionLevel;

    /*
     * State of the JoinExecStream
     */
    LhxJoinState joinState;

    /**
     * The next state of the JoinExecStream
     */
    vector<LhxJoinState> nextState;

    /*
     * Join semantics
     */
    shared_ptr<dynamic_bitset<> > joinType;

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
     * implement ExecStream
     */
    virtual void closeImpl();

    /*
     * Decide the join and setop semantics from exec stream parameters.
     */
    void setJoinType(LhxJoinExecStreamParams const &params);

    /*
     * Set up hashInfo from exec stream parameters.
     */
    void setHashInfo(LhxJoinExecStreamParams const &params);

    /*
     * Plan returns matched tuples from the probe side.
     * If curPlan is NULL, uses the default probe side where inputIndex == 0.
     */
    inline bool returnProbeInner(LhxPlan *curPlan = NULL);

    /*
     * Plan returns matched tuples from the build side.
     */
    inline bool returnBuildInner(LhxPlan *curPlan = NULL);

    /*
     * Plan returns non-matched tuples from the probe side.
     */
    inline bool returnProbeOuter(LhxPlan *curPlan = NULL);

    /*
     * Plan returns non-matched tuples from the build side.
     */
    inline bool returnBuildOuter(LhxPlan *curPlan = NULL);

    /*
     * Plan returns matched tuples from both join sides.
     */
    inline bool returnInner(LhxPlan *curPlan = NULL);

    /*
     * Plan returns tuples, matched or non-matched, from the probe side.
     */
    inline bool returnProbe(LhxPlan *curPlan = NULL);

    /*
     * Plan returns tuples, matched or non-matched, from the build side.
     */
    inline bool returnBuild(LhxPlan *curPlan = NULL);

public:
    /*
     * implement ExecStream
     */
    virtual void prepare(LhxJoinExecStreamParams const &params);

    virtual void open(bool restart);

    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);

    virtual void setResourceAllocation(
        ExecStreamResourceQuantity &quantity);
};

inline bool LhxJoinExecStream::returnProbeInner(LhxPlan *curPlan)
{
    uint probeInput = (curPlan == NULL) ? 0 : curPlan->getProbeInput();
    return joinType->test(probeInput * 2 + 0);
}

inline bool LhxJoinExecStream::returnBuildInner(LhxPlan *curPlan)
{
    uint buildInput = (curPlan == NULL) ? 1 : curPlan->getBuildInput();
    return joinType->test(buildInput * 2 + 0);
}

inline bool LhxJoinExecStream::returnProbeOuter(LhxPlan *curPlan)
{
    uint probeInput = (curPlan == NULL) ? 0 : curPlan->getProbeInput();
    return joinType->test(probeInput * 2 + 1);
}

inline bool LhxJoinExecStream::returnBuildOuter(LhxPlan *curPlan)
{
    uint buildInput = (curPlan == NULL) ? 1 : curPlan->getBuildInput();
    return joinType->test(buildInput * 2 + 1);
}

inline bool LhxJoinExecStream::returnInner(LhxPlan *curPlan)
{
    return (returnProbeInner(curPlan) && returnBuildInner(curPlan));
}

inline bool LhxJoinExecStream::returnProbe(LhxPlan *curPlan)
{
    return (returnProbeInner(curPlan) || returnProbeOuter(curPlan));
}

inline bool LhxJoinExecStream::returnBuild(LhxPlan *curPlan)
{
    return (returnBuildInner(curPlan) || returnBuildOuter(curPlan));
}

FENNEL_END_NAMESPACE

#endif

// End LhxJoinExecStream.h
