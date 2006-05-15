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

#ifndef Fennel_LhxPartition_Included
#define Fennel_LhxPartition_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/lucidera/hashexe/LhxJoinBase.h"
#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Classes to manage hash join partitions in memory and on disk.
 *
 * @author Rushan Chen
 * @version $Id$
 */
struct LhxPartition
{
    /*
     * The first page allocated from the SegStream associated with this
     * partition
     * Subsequent pages from this parition can be retrieved.
     * If firstPageId is NULL_PAGE_ID, then this partition is 
     */
    PageId firstPageId;

    /*
     * which input does data in this partition come from
     * 0 for the original probe input
     * 1 for the original build input
     */
    uint   inputIndex;

#ifdef NOT_DONE
	VarBitMap m_Filter;											// bimap filter
	SubPartStat *m_pSubPartStats;					// sub-partition statistics
	BOOL m_bOneKey;						// whether there is only 1 distinct key
#endif
};

class LhxPartitionWriter
{
    /**
     * Partition to write to.
     */
    SharedLhxPartition destPartition;

    /**
     * Helper used for writing a partition
     */
    SharedSegOutputStream pSegOutputStream;
    
    /**
     * Tuple accessor to marshal the inputTuple.
     */
    TupleAccessor tupleAccessor;
public:
    void open(SharedLhxPartition destPartition,
              LhxJoinInfo const &joinInfo);
    void marshalTuple(TupleData const &inputTuple);
    void close();
};

enum LhxPartitionReaderState {
    ReadingFromStream, ReadingFromHashTable, ReadingFromPartition 
};

class LhxPartitionReader
{
    /**
     * Partition to read from.
     */
    SharedLhxPartition srcPartition;

    /**
     * Helper used for reading a partition
     */
    SharedSegInputStream pSegInputStream;
    
    /**
     * Tuple accessor to unmarshal the disk content to the outputTuple.
     */
    TupleAccessor tupleAccessor;

    /**
     * Storage Length of last tuple read from the underlying partition.
     */
    uint tupleStorageLength;

    LhxPartitionReaderState readerState;
    bool srcIsStream;
    ExecStreamBufState bufState;

    /**
     * If reader is on a partition which comes from the input exec stream,
     * (this is when this accessor is initialized using a NULL_PAGE_ID),
     * use an exec stream buf accessor to read tuples.
     */
    SharedExecStreamBufAccessor streamBufAccessor;

public:
    void open(SharedLhxPartition srcPartition,
        LhxJoinInfo const &joinInfo,
        bool setDeallocate);
    void unmarshalTuple(TupleData &outputTuple);
    void consumeTuple();
    bool isTupleConsumptionPending();
    bool demandData();
    inline ExecStreamBufState getState() const;
    void close();
};

enum LhxPartitionState {
    PartitionUnderflow, PartitionEndOfData
};

enum LhxInputIndex {
    LeftInputIndex=0, RightInputIndex=1
};

struct LhxPartitionInfo
{
    /**
     * Hash Table Reader for the join.
     */
    LhxHashTableReader *hashTableReader;

    /**
     * Src partition reader.
     *
     * It could either be a local reader(tmpReader), or a reader passed in from
     * the exec stream. The latter is used to partition a build input. When the
     * hash table overflows, all the data from the hash table, plus the
     * remaining data from the build partition, as well as the inflight tuple
     * which caused the hash table overflow, need to be repartitioned.
     */
    LhxPartitionReader tmpReader;
    LhxPartitionReader *reader;

    /**
     * holding the tuple which is not yet inserted into hash table
     */
    TupleData rightTuple;

    /**
     * writerList.size() == numChildPart
     *
     * destPartitionList.size() == 2 * numChildPart because child partitions
     * for both inputs have to be complete before child plans can be created.
     */
    std::vector<SharedLhxPartitionWriter> writerList;
    std::vector<SharedLhxPartition> destPartitionList;

    uint curInputIndex;
    uint numChildPart;

    LhxPartitionInfo() {reader = NULL; hashTableReader = NULL;}

    /**
     * Prepare to partition the probe input which reads from a partition(which
     * could be either disk partition or execution buffer stream).
     */
    void init(uint curInputIndex,
        SharedLhxPartition partition,
        uint numChildPart,
        LhxJoinInfo const &joinInfo);
    
    /**
     * Prepare to partition the build input which reads from both hash table
     * and an existing reader. There is also a inflight tuple that is part of
     * this partition.
     */
    void init(uint curInputIndex,
        SharedLhxPartition partition,
        uint numChildPart,
        LhxJoinInfo const &joinInfo,
        LhxHashTableReader *hashTableReaderInit,
        LhxPartitionReader *rightReader,
        TupleData &rightTuple);
    
    /**
     * Close the reader stream and the writer streams.
     */
    void reset(bool resetPartition);
};

class LhxPlan
{
public:
    uint partitionLevel;
    SharedLhxPartition partition[2];

    /*
     * Plan linkage.
     * Parent plan is not a shared pointer to avoid cycles, which affect
     * shared pointer reference counting.
     */
    LhxPlan *parentPlan;
    SharedLhxPlan firstChildPlan;
    SharedLhxPlan siblingPlan;
    
    /*
     * numSubPart == keptSubPart.size() + childrenPlan.size()
     */
    uint numChildPart;
    uint numSubPart;
    boost::scoped_array<uint> keptSubPart; 
    
    ostringstream dataTrace;
    TuplePrinter tuplePrinter;

    void init(
        uint partitionLevelInit,
        uint numChildPartInit,
        SharedLhxPartition partitionInit0,
        SharedLhxPartition partitionInit1,
        LhxPlan *parentPlanInit);

    /**
     * Generate partitions for the child plans.
     */
    LhxPartitionState generatePartitions(LhxJoinInfo const &joinInfo,
        LhxPartitionInfo &partInfo, bool traceOn);

    /**
     * Partition this plan and create child plan.
     * This is used in testing only.
     */
    void createChildren(LhxJoinInfo const &joinInfo);

    /**
     * Create child plan from partitions provided via partInfo.
     */
    void createChildren(LhxPartitionInfo &partInfo);

    /**
     * Add sibling plan.
     */
    inline void addSibling(SharedLhxPlan siblingPlan);
    
    /**
     * Get first leaf plan in dfs order.
     */
    LhxPlan *getFirstLeaf();

    /**
     * Get next leaf plan in dfs order.
     */
    LhxPlan *getNextLeaf();

    /**
     * Get the partition corresponding to inputIndex.
     */
    inline SharedLhxPartition getPartition(uint inputIndex);

    /**
     * Get the first child plan.
     */
    inline SharedLhxPlan getFirstChild();
};

inline ExecStreamBufState LhxPartitionReader::getState() const
{
    if (srcIsStream) {
        return streamBufAccessor->getState();
   } else {
        return bufState;
   }
}

inline void LhxPlan::addSibling(SharedLhxPlan siblingPlanInit)
{
    siblingPlanInit->siblingPlan = siblingPlan;
    siblingPlan = siblingPlanInit;
}

inline SharedLhxPartition LhxPlan::getPartition(uint inputIndex)
{
    return partition[inputIndex];
}

inline SharedLhxPlan LhxPlan::getFirstChild()
{
    return (firstChildPlan);
}

FENNEL_END_NAMESPACE

#endif

// End LhxPartition.h
