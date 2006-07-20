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
#include <boost/shared_array.hpp>

using namespace std;
using namespace boost;

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
    void open(SharedLhxPartition destPartitionInit,
        LhxHashInfo const &hashInfo);
    void open(SharedLhxPartition destPartitionInit,
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
     * (this is when this accessor is opened on a partition without valid
     * segStream), use an exec stream buf accessor to read tuples.
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
     * Child partitions for both inputs have to be complete before child plans
     * can be created.
     *     writerList.size() == destPartitionList.size() ==
     *           numInput * LhxPlan::LhxChildPartCount
     *
     * Input filters are used to filter only one input(the build input) of each
     * child partition.
     *
     * joinFilterList.size() == LhxPlan::LhxChildPartCount
     */
    vector<SharedLhxPartitionWriter> writerList;
    vector<SharedLhxPartition> destPartitionList;
    vector<shared_ptr<dynamic_bitset<> > > joinFilterList;


    /*
     * Stats associated with the subpartitions. One set of subpartitions for
     * each input partition.
     * subPartStatList.size() == numInput * LhxPlan::LhxChildPartCount
     * and each item in the list contains LhxPlan::LhxSubPartCount elements.
     */
    vector<shared_array<uint> > subPartStatList;

    uint numInput;
    uint curInputIndex;

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
    uint partitionLevel;
    vector<SharedLhxPartition> partitions;
    vector<bool>               useJoinFilter;
    shared_ptr<dynamic_bitset<> > joinFilter;
    
    /*
     * Map sub partitions to child partitions, based on hash key.
     * The mapping algorithm tries to put similar amount of data to each child
     * partition.
     */
    bool useSubPartStat;
    shared_array<uint> subPartToChildMap;
    shared_array<uint> childPartSize;

    /*
     * Plan linkage.
     * Parent plan is not a shared pointer to avoid cycles, which affect
     * shared pointer reference counting.
     */
    LhxPlan *parentPlan;
    SharedLhxPlan firstChildPlan;
    SharedLhxPlan siblingPlan;
    
    /**
     * Add sibling plan.
     */
    inline void addSibling(SharedLhxPlan siblingPlan);

    /**
     * Using sub partition stats gathered at the previous partition level, map
     * sub partitions to child partitions. The objective is to come up with
     * child partitions of similar size.
     */
    void mapSubPartToChild(shared_array<uint> subPartStats);

public:
    /*
     * Maximum number of subpartitions a non-leaf partition has. Subpartitions
     * are packed into child partitions so that child partitions are of
     * similar size.
     */
    static const uint LhxSubPartCount = 16;
    static const uint LhxChildPartCount = 3;

    /**
     * Initialize a plan, with its input partitions and parent plan.
     */
    void init(
        uint partitionLevelInit,
        vector<SharedLhxPartition> &partitionsInit,
        LhxPlan *parentPlanInit,
        bool useSubPartStat);

    /**
     * Initialize a plan.
     */
    void init(
        uint partitionLevelInit,
        vector<SharedLhxPartition> &partitionsInit,
        bool useSubPartStat,
        vector<shared_array<uint> > &subPartStats,
        LhxPlan *parentPlanInit,
        vector<bool> useJoinFilterInit,
        shared_ptr<dynamic_bitset<> > filterInit =
                shared_ptr<dynamic_bitset<> >());

    /**
     * Calculate the atrget child partition index based on the hashkey of a
     * tuple.
     */
    uint calculateChildIndex(uint hashKey, uint curInputIndex);
    
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
     * Get the partition level of this plan.
     */
    inline uint getPartitionLevel();

    /**
     * Get the partition corresponding to inputIndex.
     */
    inline SharedLhxPartition getPartition(uint inputIndex);

    /**
     * Get the first child plan.
     */
    inline SharedLhxPlan getFirstChild();

    /**
     * Get first leaf plan in dfs order.
     */
    LhxPlan *getFirstLeaf();

    /**
     * Get next leaf plan in dfs order.
     */
    LhxPlan *getNextLeaf();

    /**
     * Close the plan tree. Release any resource pointed to from this plan
     * tree.
     */
    void close();

    /**
     * Print the content of the plan tree rooted at this plan.
     *
     * @return the string representation of this plan tree.
     */
    string toString();
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

inline void LhxPlan::addSibling(SharedLhxPlan siblingPlanInit)
{
    siblingPlan = siblingPlanInit;
}

inline uint LhxPlan::getPartitionLevel()
{
    return partitionLevel;
}

inline SharedLhxPartition LhxPlan::getPartition(uint inputIndex)
{
    return partitions[inputIndex];
}

inline SharedLhxPlan LhxPlan::getFirstChild()
{
    return firstChildPlan;
}

FENNEL_END_NAMESPACE

#endif

// End LhxPartition.h
