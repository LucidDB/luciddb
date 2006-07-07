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
#include "fennel/segment/SegStreamAllocation.h"
#include "fennel/lucidera/hashexe/LhxHashBase.h"
#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/AggComputer.h"
#include <boost/dynamic_bitset.hpp>
#include <boost/scoped_array.hpp>

using namespace std;

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
     * The seg stream pair (input/output) associated with this partition.
     */
    SharedSegStreamAllocation segStream;

    /*
     * which input does data in this partition come from
     * 0 for the original probe input
     * 1 for the original build input
     */
    uint   inputIndex;

#ifdef NOT_DONE
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

    bool isAggregate;

    /**
     * Each writer has a local hash table(which shares the scratch page quota
     * with other writer) to compute partial aggregates before flushing them to
     * disk.
     */
    LhxHashTable hashTable;
    LhxHashTableReader hashTableReader;
    TupleData partialAggTuple;

public:
    void open(SharedLhxPartition destPartition,
        LhxHashInfo const &hashInfo);
    void open(SharedLhxPartition destPartition,
        LhxHashInfo &hashInfo,
        AggComputerList *aggList,
        uint numWriterCachePages);
    inline void allocateResources();
    void marshalTuple(TupleData const &inputTuple);
    void aggAndMarshalTuple(TupleData const &inputTuple);
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
    bool srcIsInputStream;
    ExecStreamBufState bufState;
    TupleDescriptor outputTupleDesc;

    /**
     * If reader is on a partition which comes from the input exec stream,
     * (this is when this accessor is initialized using a NULL_PAGE_ID),
     * use an exec stream buf accessor to read tuples.
     */
    SharedExecStreamBufAccessor streamBufAccessor;

public:
    void open(SharedLhxPartition srcPartition,
              LhxHashInfo const &hashInfo);
    bool isTupleConsumptionPending();
    bool demandData();
    void unmarshalTuple(TupleData &outputTuple);
    void consumeTuple();
    void close();
    inline ExecStreamBufState getState() const;
    inline TupleDescriptor const &getTupleDesc() const;
};

enum LhxPartitionState {
    PartitionUnderflow, PartitionEndOfData
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
    LhxPartitionReader probeReader;
    LhxPartitionReader *reader;

    /**
     * holding the tuple which is not yet inserted into hash table
     */
    TupleData buildTuple;

    /**
     * writerList.size() == numChildPart
     *
     * destPartitionList.size() == 2 * numChildPart because child partitions
     * for both inputs have to be complete before child plans can be created.
     */
    vector<SharedLhxPartitionWriter> writerList;
    vector<SharedLhxPartition> destPartitionList;
    vector<boost::shared_ptr<boost::dynamic_bitset<> > > joinFilterList;

    uint numInput;
    uint curInputIndex;
    uint numChildPart;

    LhxHashInfo *hashInfo;

    /*
     * True if the tuples currently being partitioned come from
     * memory(i.e. from the hash table).
     */
    bool partitionMemory;

    LhxPartitionInfo() {reader = NULL; hashTableReader = NULL;}

    /**
     * Set up the recursive partitioning context.
     */
    void init(
        uint numInputInit,
        uint numChildPartInit,
        LhxHashInfo *hashInfoInit);

    /**
     * Prepare to partition the inputs:
     * Build inout reads from both hash table and an existing reader. There is
     * also a inflight tuple that is part of this partition.
     * Probe input reads from a partition, which could be either disk partition
     * or execution buffer stream.
     */
    void open(
        LhxHashTableReader *hashTableReaderInit,
        LhxPartitionReader *buildReader,
        TupleData &buildTuple,
        SharedLhxPartition probePartition);

    /**
     * Prepare to aggregate and partition the (build) input which reads from both
     * hash table and an existing reader. There is also a inflight tuple that
     * is part of this partition.
     * aggList contains the agg computers which aggregate the input.
     */
    void open(
        LhxHashTableReader *hashTableReaderInit,
        LhxPartitionReader *buildReader,
        TupleData &buildTuple,
        AggComputerList *aggList);

    /**
     * Close the reader stream and the writer streams.
     */
    void close();
};

class LhxPlan
{
public:
    uint partitionLevel;
    vector<SharedLhxPartition> partitions;
    vector<bool>               useFilter;
    boost::shared_ptr<boost::dynamic_bitset<> > joinFilter;

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
    
    inline void init(
        uint partitionLevelInit,
        uint numChildPartInit,
        vector<SharedLhxPartition> &partitionsInit,
        LhxPlan *parentPlanInit);

    void init(
        uint partitionLevelInit,
        uint numChildPartInit,
        vector<SharedLhxPartition> &partitionsInit,
        LhxPlan *parentPlanInit,
        vector<bool> useFilterInit,
        boost::shared_ptr<boost::dynamic_bitset<> > filterInit =
                boost::shared_ptr<boost::dynamic_bitset<> >());

    /**
     * Generate partitions for the child plans.
     */
    LhxPartitionState generatePartitions(
        LhxHashInfo const &hashInfo,
        LhxPartitionInfo &partInfo);

    /**
     * Partition this plan and create child plan.
     * This is used in testing only.
     */
    void createChildren(LhxHashInfo const &hashInfo);

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

    /**
     * Close the plan tree. Release any resource pointed to from this plan
     * tree.
     */
    void close();
};

inline ExecStreamBufState LhxPartitionReader::getState() const
{
    if (srcIsInputStream) {
        return streamBufAccessor->getState();
   } else {
        return bufState;
   }
}

inline TupleDescriptor const &LhxPartitionReader::getTupleDesc() const
{
    return outputTupleDesc;
}

inline void LhxPartitionWriter::allocateResources()
{
    bool status = hashTable.allocateResources();
    assert(status);
}

inline void LhxPlan::init(
    uint partitionLevelInit,
    uint numChildPartInit,   
    vector<SharedLhxPartition> &partitionsInit,
    LhxPlan *parentPlanInit)
{
    /*
     * No filter for this plan.
     */
    boost::shared_ptr<boost::dynamic_bitset<> > joinFilterInit =
        boost::shared_ptr<boost::dynamic_bitset<> >();
    vector<bool> useFilter;

    if (partitionsInit.size() == 1) {
        useFilter.push_back(false);
    }

    init(partitionLevelInit, numChildPartInit, partitionsInit, parentPlanInit,
        useFilter, joinFilterInit);
}

inline void LhxPlan::addSibling(SharedLhxPlan siblingPlanInit)
{
    siblingPlanInit->siblingPlan = siblingPlan;
    siblingPlan = siblingPlanInit;
}

inline SharedLhxPartition LhxPlan::getPartition(uint inputIndex)
{
    return partitions[inputIndex];
}

inline SharedLhxPlan LhxPlan::getFirstChild()
{
    return (firstChildPlan);
}

FENNEL_END_NAMESPACE

#endif

// End LhxPartition.h
