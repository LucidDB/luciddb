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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/hashexe/LhxJoinExecStream.h"
#include "fennel/segment/Segment.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxJoinExecStream::prepare(
    LhxJoinExecStreamParams const &params)
{
    assert (params.leftKeyProj.size() == params.rightKeyProj.size());

    ConfluenceExecStream::prepare(params);

    setJoinType(params);    
    setHashInfo(params);

    uint numInputs = inAccessors.size();

    inputTuple.reset(new TupleData[2]);
    inputTupleSize.reset(new uint[2]);

    for (int inputIndex = 0; inputIndex < numInputs; inputIndex ++ ) {
        inputTuple[inputIndex].compute(
            inAccessors[inputIndex]->getTupleDesc());
        inputTupleSize[inputIndex] = inputTuple[inputIndex].size();

        if (hashInfo.removeDuplicate[inputIndex]) {
            assert (inputTupleSize[inputIndex] ==
                hashInfo.keyProj[inputIndex].size());
        }
    }
    
    /*
     * Force partitioning level. Only set in tests.
     */
    forcePartitionLevel = params.forcePartitionLevel;
    enableSubPartStat = params.enableSubPartStat;
    
    /*
     * NOTE: currently anti joins that need to remove duplicates can not
     * switch join sides(join then
     * effectively becomes LeftAnti) because the hash table is used to remove
     * duplicated non-matched tuples. The "Anti" side has to be the build side.
     * It is difficult, thought not impossible,
     * to remove duplicates of non-matched tuples from the probe side.
     *
     * One approach to solve this is to insert the non-matched probe tuple into
     * the hash table, mark it as matched, and return this tuple. Subsequent
     * identical probe tuple will see the tuple as a "match" and will not
     * return the tuple(hence satisfy the anti join semantics).
     * This scheme also works when the hash table overflows. Tuples in the
     * hash table, including the probe tuples inserted, will be partitioned 
     * as child partitions of the build input.
     * The remaining probe input, together with all the matched tuples from the
     * hash table, will be partitioned to disk as the children of the probe
     * input. Note matched tuples are stored in both input.
     *
     * This partitioning scheme makes sure that the join result is correct
     * using the above described LeftAnti join algorithm or the already
     * implemented RightAnti join algorithm, regardless of input assignment for
     * the next partition level.
     *
     * RightAnti join without duplicate removal can use swing.
     * LeftAnti join without duplicate removal can use swing.
     *
     * RightAnti join with duplicate removal cannot use swing.
     * LeftAnti join with duplicate removal is not supported(see setJoinType())
     *
     */
    bool leftAntiJoin =
        (returnProbeOuter() && !returnProbeInner() && !returnBuild());

    bool rightAntiJoin =
        (returnBuildOuter() && !returnBuildInner() && !returnProbe());

    bool antiJoin = leftAntiJoin || rightAntiJoin;

    enableSwing = params.enableSwing && (!(antiJoin && setopDistinct));

    /* 
     * Calculate the number of blocks required to perform the join, as given by
     * the optimizer, completely in memory.
     */
    hashTable.calculateSize(hashInfo, DefaultBuildInputIndex,
        numBlocksHashTable);

    TupleDescriptor outputDesc;

    if (params.outputProj.size() != 0) {
        outputDesc.projectFrom(params.outputTupleDesc, params.outputProj);
    } else {
        outputDesc = params.outputTupleDesc;
    }

    outputTuple.compute(outputDesc);    

    assert (outputTuple.size() == (inputTupleSize[0] + inputTupleSize[1]) ||
        outputTuple.size() == inputTupleSize[0]||
        outputTuple.size() == inputTupleSize[1]);
            
    pOutAccessor->setTupleShape(outputDesc);

    /*
     * Set aside 1 cache block per child partition writer for I/O.
     */
    numMiscCacheBlocks = LhxPlan::LhxChildPartCount * numInputs;
}

void LhxJoinExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConfluenceExecStream::getResourceRequirements(minQuantity,optQuantity);

    minQuantity.nCachePages += 
        LhxHashTable::LhxHashTableMinPages + numMiscCacheBlocks;
    optQuantity.nCachePages += numBlocksHashTable + numMiscCacheBlocks;
    /*
     * TODO(rchen 2006-08-08): use the real min above.
     */
    minQuantity.nCachePages = optQuantity.nCachePages;
}

void LhxJoinExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    ConfluenceExecStream::setResourceAllocation(quantity);
    hashInfo.numCachePages = quantity.nCachePages - numMiscCacheBlocks;
}

void LhxJoinExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);

    if (restart) {
        hashTable.releaseResources();
    };

    uint partitionLevel = 0;

    /*
     * Create the root plan.
     *
     * The execute state machine operates at the plan level.
     */
    probePart = SharedLhxPartition(new LhxPartition());
    buildPart = SharedLhxPartition(new LhxPartition());

    (probePart->segStream).reset();
    probePart->inputIndex = DefaultProbeInputIndex;

    (buildPart->segStream).reset();
    buildPart->inputIndex = DefaultBuildInputIndex;

    LhxPlan *parentPlan = NULL;

    vector<SharedLhxPartition> partitionList;
    partitionList.push_back(probePart);
    partitionList.push_back(buildPart);

    vector<shared_array<uint> > subPartStats;
    subPartStats.push_back(shared_array<uint>());
    subPartStats.push_back(shared_array<uint>());

    shared_ptr<dynamic_bitset<> > joinFilterInit =
        shared_ptr<dynamic_bitset<> >();

    vector<uint> filteredRows;
    filteredRows.push_back(0);
    filteredRows.push_back(0);
    
    /*
     * No input join filter for root plan.
     */
    rootPlan =  SharedLhxPlan(new LhxPlan());
    rootPlan->init(parentPlan, partitionLevel, partitionList, subPartStats,
        joinFilterInit, filteredRows,  enableSubPartStat, enableSwing);

    /*
     * Initialize recursive partitioning context.
     */
    partInfo.init(&hashInfo);

    curPlan = rootPlan.get();
    isTopPlan = true;

    hashTable.init(curPlan->getPartitionLevel(), hashInfo,
        curPlan->getBuildInput());
    hashTableReader.init(&hashTable, hashInfo, curPlan->getBuildInput());
    
    bool status = hashTable.allocateResources();
    assert (status);

    buildReader.open(curPlan->getBuildPartition(), hashInfo);

    joinState = (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
    nextState.clear();
}

ExecStreamResult LhxJoinExecStream::execute(ExecStreamQuantum const &quantum)
{
    while (true)
    {
        switch (joinState)
        {
        case ForcePartitionBuild:
            {
                TupleData &buildTuple = inputTuple[curPlan->getBuildInput()];

                /*
                 * Build
                 */
                for (;;) {
                    if (!buildReader.isTupleConsumptionPending()) {
                        if (buildReader.getState() == EXECBUF_EOS) {
                            /*
                             * break out of this loop, and start probing.
                             */
                            buildReader.close();
                            probeReader.open(curPlan->getProbePartition(),
                                hashInfo);
                            joinState = Probe;
                            numTuplesProduced = 0;
                            break;
                        }

                        if (!buildReader.demandData()) {
                            if (isTopPlan) {
                                /*
                                 * Top level: request more data from producer.
                                 */
                                return EXECRC_BUF_UNDERFLOW;
                            } else {
                                /*
                                 * Recursive level: no more data in partition.
                                 * Come back to the top of the same state to
                                 * detect EOS.
                                 */
                                break;
                            }
                        }
                        buildReader.unmarshalTuple(buildTuple);
                    }

                    /*
                     * Add tuple to hash table.
                     *
                     * NOTE: This is a testing state. Always partition up to
                     * forcePartitionLevel.
                     */
                    if (curPlan->getPartitionLevel() < forcePartitionLevel ||
                        !hashTable.addTuple(buildTuple)) {
                        /*
                         * If hash table is full, partition input data.
                         *
                         * First, partition the right(build input).
                         */
                        partInfo.open(
                            &hashTableReader, &buildReader, buildTuple,
                            curPlan->getProbePartition(),
                            curPlan->getBuildInput());
                        joinState = Partition;
                        break;
                    }
                    buildReader.consumeTuple();
                }
                break;
            }
        case Build:
            {
                TupleData &buildTuple = inputTuple[curPlan->getBuildInput()];

                /*
                 * Build
                 */
                for (;;) {
                    if (!buildReader.isTupleConsumptionPending()) {
                        if (buildReader.getState() == EXECBUF_EOS) {
                            /*
                             * break out of this loop, and start probing.
                             */
                            buildReader.close();
                            probeReader.open(curPlan->getProbePartition(),
                                hashInfo);
                            joinState = Probe;
                            numTuplesProduced = 0;
                            break;
                        }

                        if (!buildReader.demandData()) {
                            if (isTopPlan) {
                                /*
                                 * Top level: request more data from producer.
                                 */
                                return EXECRC_BUF_UNDERFLOW;
                            } else {
                                /*
                                 * Recursive level: no more data in partition.
                                 * Come back to the top of the same state to
                                 * detect EOS.
                                 */
                                break;
                            }
                        }
                        buildReader.unmarshalTuple(buildTuple);
                    }

                    /*
                     * Add tuple to hash table.
                     */
                    if (!hashTable.addTuple(buildTuple)) {
                        /*
                         * If hash table is full, partition input data.
                         *
                         * First, partition the right(build input).
                         */
                        partInfo.open(
                            &hashTableReader, &buildReader, buildTuple,
                            curPlan->getProbePartition(),
                            curPlan->getBuildInput());
                        joinState = Partition;
                        break;
                    }
                    buildReader.consumeTuple();
                }
                break;
            }
        case Partition:
            {
                for (;;) {
                    if (curPlan->generatePartitions(hashInfo, partInfo)
                        == PartitionUnderflow) {
                        /*
                         * Request more data from producer.
                         */
                        return EXECRC_BUF_UNDERFLOW;
                    } else {
                        /*
                         * Finished building the partitions for both
                         * inputs.
                         */
                        break;
                    }
                }
                partInfo.close();
                joinState = CreateChildPlan;
                break;
            }
        case CreateChildPlan:
            {
                /*
                 * Link the newly created partitioned in the plan tree.
                 */
                curPlan->createChildren(partInfo, enableSubPartStat,
                    enableSwing);

                FENNEL_TRACE(TRACE_FINE, curPlan->toString());
                
                /*
                 * now recursice down the plan tree to get the first leaf plan.
                 */
                curPlan = curPlan->getFirstChild().get();
                isTopPlan = false;

                hashTable.releaseResources();

                hashTable.init(curPlan->getPartitionLevel(), hashInfo,
                    curPlan->getBuildInput());
                hashTableReader.init(&hashTable, hashInfo,
                    curPlan->getBuildInput());

                bool status = hashTable.allocateResources();
                assert (status);
                buildReader.open(curPlan->getBuildPartition(), hashInfo);

                joinState = 
                    (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
                nextState.clear();
                break;                
            }
        case GetNextPlan:
            {
                hashTable.releaseResources();
                curPlan = curPlan->getNextLeaf();

                if (curPlan) {
                    hashTable.init(curPlan->getPartitionLevel(), hashInfo,
                        curPlan->getBuildInput());
                    hashTableReader.init(&hashTable, hashInfo,
                        curPlan->getBuildInput());

                    bool status = hashTable.allocateResources();
                    assert (status);
                    buildReader.open(curPlan->getBuildPartition(), hashInfo);
                    joinState =
                        (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
                    nextState.clear();
                } else {
                    joinState = Done;
                }
                break;
            }
        case Probe:
            {
                TupleData &probeTuple = inputTuple[curPlan->getProbeInput()];
                uint probeTupleSize = inputTupleSize[curPlan->getProbeInput()];
                TupleProjection &probeKeyProj  = (TupleProjection &)
                    hashInfo.keyProj[curPlan->getProbeInput()];
                uint buildTupleSize = inputTupleSize[curPlan->getBuildInput()];
                bool removeDuplicateProbe =
                    hashInfo.removeDuplicate[curPlan->getProbeInput()];
                bool filterNullProbe = regularJoin;
                uint probeFieldOffset =
                    returnBuild(curPlan) ?
                    buildTupleSize * curPlan->getProbeInput() : 0;
                uint buildFieldOffset =
                    returnProbe(curPlan) ?
                    probeTupleSize * curPlan->getBuildInput() : 0;
                uint probeFieldLength =
                    returnProbe(curPlan) ? probeTupleSize : 0;
                uint buildFieldLength =
                    returnBuild(curPlan) ? buildTupleSize : 0;

                /*
                 * Probe
                 */
                for (;;) {
                    if (!probeReader.isTupleConsumptionPending()) {
                        if (probeReader.getState() == EXECBUF_EOS) {
                            probeReader.close();
                            if (returnBuildOuter(curPlan)) {
                                /*
                                 * Join types that return non-matching
                                 * tuples from the build input: RightOuter,
                                 * FullOuter, RightAnti,
                                 *
                                 * Set the output tuple to have NULL values on
                                 * the left(probe side), and return all the
                                 * non-matching tuples in the hash table on the
                                 * right.
                                 */
                                hashTableReader.bindUnMatched();

                                /*
                                 * fill in the probe side, if required, with
                                 * NULLs
                                 */
                                for (uint i = 0; i < probeFieldLength; i ++) {
                                    outputTuple[i + probeFieldOffset].pData = NULL;
                                }
                                joinState = ProduceBuild;
                                nextState.push_back(GetNextPlan);
                            } else {
                                /*
                                 * Probing for this plan is done.
                                 */
                                joinState = GetNextPlan;
                            }
                            break;
                        }
                        if (!probeReader.demandData()) {
                            if (isTopPlan) {
                                /*
                                 * Top level: request more data from producer.
                                 */
                                return EXECRC_BUF_UNDERFLOW;
                            } else {
                                /*
                                 * Recursive level: no more data in partition.
                                 * Come back to the top of the same state to
                                 * detect EOS.
                                 */
                                break;
                            }
                        }
                        probeReader.unmarshalTuple(probeTuple);
                    }

                    PBuffer keyBuf = NULL;

                    /*
                     * Try to locate matching key in the hash table.
                     * If this tuple does contain null in its key columns, it
                     * will not join so hash table lookup is not needed.
                     */
                    if (!filterNullProbe ||
                        !probeTuple.containsNull(probeKeyProj)) {
                        keyBuf =
                            hashTable.findKey(probeTuple, probeKeyProj, 
                                removeDuplicateProbe);
                    }
        
                    if (keyBuf) {
                        if (returnBuildInner(curPlan)) {
                            /*
                             * Join types that return matching tuples from both
                             * inputs: InnerJoin, LeftOuter, RightOuter,
                             * FullOuter
                             *
                             * Set the output tuple to include only the probe input,
                             * and get the next matching tuple from the build side.
                             */
                            for (uint i = 0; i < probeFieldLength; i ++) {
                                outputTuple[i + probeFieldOffset].copyFrom(probeTuple[i]);
                            }

                            /**
                             * Output the joined tuple.
                             */
                            hashTableReader.bindKey(keyBuf);
                            joinState = ProduceBuild;
                            nextState.push_back(Probe);
                            break;
                        } else if (returnProbeInner(curPlan) &&
                            !returnProbeOuter() && !returnBuild(curPlan)) {
                            /*
                             * Join types that return (distinct) matching
                             * tuples from the probe input: LeftSemiJoin
                             *
                             * Currently LeftSemiJoin is only used in set
                             * matching semantics producing one output tuple
                             * per matched tuple from the left side.
                             *
                             * Set the output tuple to include only the probe input.
                             */
                            for (uint i = 0; i < probeFieldLength; i ++) {
                                outputTuple[i + probeFieldOffset].copyFrom(probeTuple[i]);
                            }
                            joinState = ProducePending;
                            nextState.push_back(Probe);
                            break;
                        } else {
                            /*
                             * RightAnti falls through here.
                             * Go back to match all probing rows and return
                             * non-matched tuples from the hash table.
                             */
                            probeReader.consumeTuple();
                        }
                    } else {
                        /*
                         * No match. Need to return the leftTuple if leftOuter
                         * join.
                         */
                        if (returnProbeOuter(curPlan)) {
                            /*
                             * Join types that return non-matching
                             * tuples from the probe input: LeftOuter, FullOuter
                             *
                             * Set the output tuple to include only the left
                             * input, and set NULL values on the right.
                             */
                            for (uint i = 0; i < probeFieldLength; i ++) {
                                outputTuple[i + probeFieldOffset].copyFrom(probeTuple[i]);
                            }

                            for (uint i = 0; i < buildFieldLength; i ++) {
                                outputTuple[i + buildFieldOffset].pData = NULL;
                            }
                            joinState = ProducePending;
                            nextState.push_back(Probe);
                            break;
                        } else {
                            probeReader.consumeTuple();
                        }
                    }
                }
                break;
            }
        case ProduceBuild:
            {
                TupleData &buildTuple = inputTuple[curPlan->getBuildInput()];
                uint probeTupleSize = inputTupleSize[curPlan->getProbeInput()];
                uint buildTupleSize = inputTupleSize[curPlan->getBuildInput()];
                uint buildFieldOffset =
                    returnProbe(curPlan) ?
                    probeTupleSize * curPlan->getBuildInput() : 0;
                uint buildFieldLength =
                    returnBuild(curPlan) ? buildTupleSize : 0;

                /*
                 * Producing the results.
                 * Handle output overflow and quantum expiration in
                 * ProducePending state.
                 */
                if (hashTableReader.getNext(buildTuple)) {
                    for (uint i = 0; i < buildFieldLength; i ++) {
                        outputTuple[i + buildFieldOffset].copyFrom(buildTuple[i]);
                    }
                    
                    joinState = ProducePending;
                    /*
                     * Come back to this state after producing the output tuple
                     * successfully.
                     */
                    nextState.push_back(ProduceBuild);
                } else {
                    joinState = nextState.back();
                    nextState.pop_back();
                    if (joinState == Probe) {
                        probeReader.consumeTuple();
                    }
                }
                break;
            }
        case ProducePending:
            {
                if (pOutAccessor->produceTuple(outputTuple)) {
                    numTuplesProduced++;
                    joinState = nextState.back();
                    nextState.pop_back();
                    if (joinState == Probe) {
                        probeReader.consumeTuple();
                    }
                } else {
                    numTuplesProduced = 0;
                    return EXECRC_BUF_OVERFLOW;                    
                }

                /*
                 * Successfully produced an output row. Now check if quantum
                 * has expired.
                 */
                if (numTuplesProduced >= quantum.nTuplesMax) {
                    /*
                     * Reset count.
                     */
                    numTuplesProduced = 0;
                    return EXECRC_QUANTUM_EXPIRED;
                }
                break;
            }
        case Done:
            {
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
        }
    }
    
    /*
     * The state machine should never come here.
     */
    assert (false);
}

void LhxJoinExecStream::closeImpl()
{
    hashTable.releaseResources();
    if (rootPlan) {
        rootPlan->close();
        rootPlan.reset();
    }
    ConfluenceExecStream::closeImpl();
}

void LhxJoinExecStream::setJoinType(
    LhxJoinExecStreamParams const &params)
{
    /*
     * Join types currently supported:
     *
     * Inner, Left Outer, Right Outer, Full Outer
     * Right Anti(return non-matching rows from the build side)
     * Left Semi(return matching rows from the probe side)
     *
     * These join types are marked by using the above four parameters. Each
     * specify whether to return matching or nonmatching tuples from a join
     * input.
     *
     * LeftInner LeftOuter RightInner RightOuter   Join Type
     *     F         T          F          F      (Left Anti)
     *     F         F          F          T       Right Anti
     *     T         F          F          F       Left Semi
     *     F         F          T          F      (Right Semi)
     *     T         F          T          F       Inner Join
     *     T         F          T          T       Right Outer
     *     T         T          T          F       Left Outer
     *     T         T          T          T       Full Outer       
     * Note join types in () are not visiible in optimizer plan.
     */

    joinType.reset(new dynamic_bitset<>(4));

    joinType->set(0, params.leftInner);
    joinType->set(1, params.leftOuter);
    joinType->set(2, params.rightInner);
    joinType->set(3, params.rightOuter);

    /*
     * By construction, at most one of the above six is true for a combination
     * of values.
     * Now make sure at least one of them is true.
     * Otherwise, the optimizer has passed in a join type not supported by this
     * join implementation.
     */
    assert (joinType->count() != 0);

    regularJoin   = !params.setopDistinct && !params.setopAll;
    setopDistinct =  params.setopDistinct && !params.setopAll;
    setopAll      = !params.setopDistinct &&  params.setopAll;

    assert (!setopAll && (regularJoin || setopDistinct));
    
    /*
     * Anit joins with duplicate removal needs to use hash table to remove
     * duplicated non-matching tuples. Hence the anti side needs to be the
     * build input(original right side).
     */
    bool leftAnti =
        (returnProbeOuter() && !returnProbeInner() && !returnBuild());

    assert (!(leftAnti && setopDistinct));
}

void LhxJoinExecStream::setHashInfo(
    LhxJoinExecStreamParams const &params)
{
    uint numInputs = inAccessors.size();
    for (int inputIndex = 0; inputIndex < numInputs; inputIndex ++ ) {
        hashInfo.streamBufAccessor.push_back(inAccessors[inputIndex]);
        hashInfo.inputDesc.push_back(
            inAccessors[inputIndex]->getTupleDesc());
        /*
         * Join types LEFTSEMI and RIGHTANTI are used exclusively in
         * set(distinct) matching operations, which by default eliminate
         * duplicates.
         */
        hashInfo.removeDuplicate.push_back(setopDistinct);
        hashInfo.numRows.push_back(params.numRows);
        hashInfo.cndKeys.push_back(params.cndKeys);
    }

    /*
     * Nulls do not join, unless in set operation.
     * Filter null values if non-matching tuples are not needed.
     */
    hashInfo.filterNull.push_back(regularJoin && !returnProbeOuter());
    hashInfo.filterNull.push_back(regularJoin && !returnBuildOuter());

    hashInfo.keyProj.push_back(params.leftKeyProj);
    hashInfo.keyProj.push_back(params.rightKeyProj);

    hashInfo.useJoinFilter.push_back(
        params.enableJoinFilter && !returnProbeOuter());
    hashInfo.useJoinFilter.push_back(
        params.enableJoinFilter && !returnBuildOuter());

    hashInfo.memSegmentAccessor = params.scratchAccessor;
    hashInfo.externalSegmentAccessor.pCacheAccessor = params.pCacheAccessor;
    hashInfo.externalSegmentAccessor.pSegment = params.pTempSegment;

    for (int inputIndex = 0; inputIndex < numInputs; inputIndex ++ ) {

        TupleProjection &keyProj  =
            (TupleProjection &)hashInfo.keyProj[inputIndex];
        TupleDescriptor &inputDesc  =
            (TupleDescriptor &)hashInfo.inputDesc[inputIndex];

        vector<bool> isKeyVarChar;
        TupleProjection dataProj;

        /*
         * Hashing is special for varchar types(the trailing blanks are
         * insignificant).
         */
        for (int j = 0; j < keyProj.size(); j ++) {
            if (inputDesc[keyProj[j]].pTypeDescriptor->getOrdinal()
                == STANDARD_TYPE_VARCHAR) {
                isKeyVarChar.push_back(true);
            } else {
                isKeyVarChar.push_back(false);
            }
        }

        hashInfo.isKeyColVarChar.push_back(isKeyVarChar);

        /*
         * Need to construct a covering set of keys; for example:
         * keyProj (3,4,2,3) should have a covering set of (3,4,2);
         */    
        for (int i = 0; i < inputDesc.size(); i ++) {
            /*
             * Okay a dumb for loop to search for key columns.
             */
            bool colIsKey = false;
            for (int j = 0; j < keyProj.size(); j ++) {
                if (i == keyProj[j]) {
                    colIsKey = true;
                    break;
                }
            }
            if (!colIsKey) {
                dataProj.push_back(i);
            }
        }
        hashInfo.dataProj.push_back(dataProj);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LhxJoinExecStream.cpp
