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
    int i, j;

    setJoinType(params);
    
    ConfluenceExecStream::prepare(params);

    for (i = 0; i < inAccessors.size() ; i ++) {
        hashInfo.streamBufAccessor.push_back(inAccessors[i]);
        hashInfo.inputDesc.push_back(inAccessors[i]->getTupleDesc());
    }

    TupleDescriptor &leftDesc  =
        (TupleDescriptor &)hashInfo.inputDesc[LeftInputIndex];
    TupleDescriptor &rightDesc =
        (TupleDescriptor &)hashInfo.inputDesc[RightInputIndex];

    leftTuple.compute(leftDesc);
    rightTuple.compute(rightDesc);
    leftTupleSize = leftTuple.size();
    rightTupleSize = rightTuple.size();

    /*
     * These two join types are used exclusively in set matching operations,
     * which by default eliminate duplicates.
     */
    removeDuplicateProbe = removeDuplicateBuild = setopDistinct;

    /*
     * Nulls do not join, unless in set operation.
     * Filter null values if non-matching tuples are not needed.
     */
    leftFilterNull  = regularJoin;

    rightFilterNull = regularJoin && !rightOuter && !fullOuter;
    
    /*
     * Force partitioning level. Only set in tests.
     */
    forcePartitionLevel = params.forcePartitionLevel;

    /*
     * Set special hash table properties.
     */
    hashInfo.filterNull = rightFilterNull;

    hashInfo.removeDuplicate = removeDuplicateBuild;

    if (hashInfo.removeDuplicate) {
        assert(rightTupleSize == params.rightKeyProj.size());
    }

    assert (params.leftKeyProj.size() == params.rightKeyProj.size());

    hashInfo.keyProj.push_back(params.leftKeyProj);
    hashInfo.keyProj.push_back(params.rightKeyProj);
    
    TupleProjection &leftKeyProj  =
        (TupleProjection &)hashInfo.keyProj[LeftInputIndex];
    TupleProjection &rightKeyProj  =
        (TupleProjection &)hashInfo.keyProj[RightInputIndex];
    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;
    vector<bool> isLeftKeyVarChar;
    vector<bool> isRightKeyVarChar;

    for (j = 0; j < leftKeyProj.size(); j ++) {
        keyDesc.push_back(leftDesc[leftKeyProj[j]]);
        /*
         * Hashing is special for varchar types(the trailing blanks are
         * insignificant).
         */
        if (leftDesc[leftKeyProj[j]].pTypeDescriptor->getOrdinal()
            == STANDARD_TYPE_VARCHAR) {
            isLeftKeyVarChar.push_back(true);
        } else {
            isLeftKeyVarChar.push_back(false);
        }
    }

    /*
     * Need to construct a covering set of keys; for example:
     * keyProj (3,4,2,3) should have a covering set of (3,4,2);
     */    
    for (i = 0; i < rightTupleSize; i ++) {
        /*
         * Okay a dumb for loop to search for key columns.
         */
        bool colIsKey = false;
        for (j = 0; j < rightKeyProj.size(); j ++) {
            if (i == rightKeyProj[j]) {
                colIsKey = true;
                break;
            }
        }
        if (!colIsKey) {
            dataDesc.push_back(rightDesc[i]);
            hashInfo.dataProj.push_back(i);
        }
    }

    for (j = 0; j < rightKeyProj.size(); j++) {
        /*
         * Hashing is special for varchar types(the trailing blanks are
         * insignificant).
         */
        if (rightDesc[rightKeyProj[j]].pTypeDescriptor->getOrdinal()
            == STANDARD_TYPE_VARCHAR) {
            isRightKeyVarChar.push_back(true);
        } else {
            isRightKeyVarChar.push_back(false);
        }
    }

    hashInfo.isKeyColVarChar.push_back(isLeftKeyVarChar);
    hashInfo.isKeyColVarChar.push_back(isRightKeyVarChar);

    /*
     * Let hash table use at most 50% of the allocated cache size.
     */
    uint cacheLimit = 
        (params.scratchAccessor.pCacheAccessor)->getCache()
        ->getMaxLockedPages() / 2;

    uint usablePageSize = 
        (params.scratchAccessor.pSegment)->getUsablePageSize();

    /* 
     * Calculate the number of blocks required to perform the join, as given by
     * the optimizer, completely in memory.
     */
    hashTable.calculateSize(
        params.numRows, params.cndKeys, 
        keyDesc, dataDesc, usablePageSize, cacheLimit,
        numBlocksHashTable);

    TupleDescriptor outputDesc;

    if (params.outputProj.size() != 0) {
        outputDesc.projectFrom(params.outputTupleDesc, params.outputProj);
    } else {
        outputDesc = params.outputTupleDesc;
    }

    outputTuple.compute(outputDesc);    
    pOutAccessor->setTupleShape(outputDesc);

    hashInfo.memSegmentAccessor = params.scratchAccessor;
    hashInfo.externalSegmentAccessor.pCacheAccessor = params.pCacheAccessor;
    hashInfo.externalSegmentAccessor.pSegment = params.pTempSegment;

    /*
     * Set aside 10 cache blocks for I/O.
     */
    numMiscCacheBlocks = 10;
}

void LhxJoinExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConfluenceExecStream::getResourceRequirements(minQuantity,optQuantity);

    minQuantity.nCachePages += numBlocksHashTable + numMiscCacheBlocks;

    optQuantity = minQuantity;
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

    hashTable.init(partitionLevel, hashInfo);
    hashTableReader.init(&hashTable, hashInfo);
    
    bool status = hashTable.allocateResources();
    assert(status);

    /*
     * Create the root plan.
     *
     * The execute state machine operates at the plan level.
     */
    leftPart = SharedLhxPartition(new LhxPartition());
    rightPart = SharedLhxPartition(new LhxPartition());

    (leftPart->segStream).reset();
    leftPart->inputIndex = LeftInputIndex;

    (rightPart->segStream).reset();
    rightPart->inputIndex = RightInputIndex;

    uint numInput       = 2;
    uint numChild       = 3;
    LhxPlan *parentPlan = NULL;

    vector<SharedLhxPartition> partitionList;
    partitionList.push_back(leftPart);
    partitionList.push_back(rightPart);

    vector<bool> useFilter;
    useFilter.push_back(!leftOuter && !fullOuter);
    useFilter.push_back(!rightOuter && !fullOuter && !rightAnti);

    /*
     * No input join filter for root plan.
     */
    rootPlan =  SharedLhxPlan(new LhxPlan());
    rootPlan->init(partitionLevel, numChild, partitionList, parentPlan,
        useFilter);

    /*
     * Initialize recursive partitioning context.
     */
    isTopPartition = true;
    partInfo.init(numInput, numChild, &hashInfo);

    curPlan = rootPlan.get();
    rightReader.open(curPlan->getPartition(RightInputIndex), hashInfo);

    joinState = (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
}

ExecStreamResult LhxJoinExecStream::execute(ExecStreamQuantum const &quantum)
{
    TupleProjection &leftKeyProj  =
        (TupleProjection &)hashInfo.keyProj[LeftInputIndex];

    while (true)
    {
        switch (joinState)
        {
        case ForcePartitionBuild:
            {
                /*
                 * Build
                 */
                for (;;) {
                    if (!rightReader.isTupleConsumptionPending()) {
                        if (rightReader.getState() == EXECBUF_EOS) {
                            /*
                             * break out of this loop, and start probing.
                             */
                            rightReader.close();
                            leftReader.open(
                                curPlan->getPartition(LeftInputIndex),
                                hashInfo);
                            joinState = Probe;
                            numTuplesProduced = 0;
                            break;
                        }

                        if (!rightReader.demandData()) {
                            if (isTopPartition) {
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
                        rightReader.unmarshalTuple(rightTuple);
                    }

                    /*
                     * Add tuple to hash table.
                     *
                     * NOTE: This is a testing state. Always partition up to
                     * forcePartitionLevel.
                     */
                    if (curPlan->partitionLevel < forcePartitionLevel ||
                        !hashTable.addTuple(rightTuple)) {
                        /*
                         * If hash table is full, partition input data.
                         *
                         * First, partition the right(build input).
                         */
                        partInfo.open(
                            &hashTableReader, &rightReader, rightTuple,
                            curPlan->getPartition(LeftInputIndex));
                        joinState = Partition;
                        break;
                    }
                    rightReader.consumeTuple();
                }
                break;
            }
        case Build:
            {
                /*
                 * Build
                 */
                for (;;) {
                    if (!rightReader.isTupleConsumptionPending()) {
                        if (rightReader.getState() == EXECBUF_EOS) {
                            /*
                             * break out of this loop, and start probing.
                             */
                            rightReader.close();
                            leftReader.open(
                                curPlan->getPartition(LeftInputIndex),
                                hashInfo);
                            joinState = Probe;
                            numTuplesProduced = 0;
                            break;
                        }

                        if (!rightReader.demandData()) {
                            if (isTopPartition) {
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
                        rightReader.unmarshalTuple(rightTuple);
                    }

                    /*
                     * Add tuple to hash table.
                     */
                    if (!hashTable.addTuple(rightTuple)) {
                        /*
                         * If hash table is full, partition input data.
                         *
                         * First, partition the right(build input).
                         */
                        partInfo.open(
                            &hashTableReader, &rightReader, rightTuple,
                            curPlan->getPartition(LeftInputIndex));
                        joinState = Partition;
                        break;
                    }
                    rightReader.consumeTuple();
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
                curPlan->createChildren(partInfo);
                
                /*
                 * now recursice down the plan tree to get the first leaf plan.
                 */
                curPlan = curPlan->getFirstChild().get();

                isTopPartition = false;
                hashTable.releaseResources();

                hashTable.init(curPlan->partitionLevel, hashInfo);
                hashTableReader.init(&hashTable, hashInfo);

                bool status = hashTable.allocateResources();
                assert(status);
                rightReader.open(
                    curPlan->getPartition(RightInputIndex),
                    hashInfo);

                joinState = (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
                break;                
            }
        case GetNextPlan:
            {
                curPlan = curPlan->getNextLeaf();
                if (curPlan) {
                    hashTable.releaseResources();

                    hashTable.init(curPlan->partitionLevel, hashInfo);
                    hashTableReader.init(&hashTable, hashInfo);

                    bool status = hashTable.allocateResources();
                    assert(status);
                    rightReader.open(
                        curPlan->getPartition(RightInputIndex),
                        hashInfo);
                    joinState =
                        (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
                } else {
                    joinState = Done;
                }
                break;
            }
        case Probe:
            {
                /*
                 * Probe
                 */
                for (;;) {
                    if (!leftReader.isTupleConsumptionPending()) {
                        if (leftReader.getState() == EXECBUF_EOS) {
                            leftReader.close();
                            if (rightOuter || fullOuter) {
                                /*
                                 * Set the output tuple to have NULL values on
                                 * the left, and return all the
                                 * non-matching tuples in the hash table on the
                                 * right.
                                 */
                                hashTableReader.bindUnMatched();

                                for (int i = 0; i <leftTupleSize; i ++) {
                                    outputTuple[i].pData = NULL;
                                }

                                joinState = ProduceRightOuter;
                            } else if (rightAnti) {
                                /*
                                 * Return non-matched rows from this plan
                                 * before moving to the next plan.
                                 */
                                hashTableReader.bindUnMatched();
                                joinState = ProduceRightAnti;
                            } else {
                                /*
                                 * Probing for this plan is done.
                                 */
                                joinState = GetNextPlan;
                            }
                            break;
                        }
                        if (!leftReader.demandData()) {
                            if (isTopPartition) {
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
                        leftReader.unmarshalTuple(leftTuple);
                    }

                    PBuffer keyBuf = NULL;

                    /*
                     * Try to locate matching key in the hash table.
                     * If this tuple does contain null in its key columns, it
                     * will not join so hash table lookup is not needed.
                     */
                    /*
                     * FIXME: set matching semantics
                     */
                    keyBuf =
                        hashTable.findKey(leftTuple, leftKeyProj,
                            leftFilterNull, removeDuplicateProbe);
        
                    if (keyBuf) {
                        /*
                         * Set the output tuple to include only the left input,
                         * and get the next matching tuple from the right.
                         */
                        for (int i = 0; i <leftTupleSize; i ++) {
                            outputTuple[i].copyFrom(leftTuple[i]);
                        }
                        
                        if (innerJoin || leftOuter || rightOuter || fullOuter) {
                            /**
                             * Output the joined tuple.
                             */
                            hashTableReader.bindKey(keyBuf);
                            joinState = ProduceInner;
                            break;
                        } else if (leftSemi) {
                            joinState = ProduceLeftSemi;
                            nextState = ProducePending;
                            break;
                        } else {
                            /*
                             * rightAnti join falls through here.
                             * It goes back to match all probing rows and return
                             * non-matched rows from the hash table after
                             * exhausting all probing rows.
                             */
                            leftReader.consumeTuple();
                        }
                    } else {
                        /*
                         * No match. Need to return the leftTuple if leftOuter
                         * join.
                         */
                        if (!leftOuter && !fullOuter) {
                            leftReader.consumeTuple();
                        } else {
                            /*
                             * Set the output tuple to include only the left
                             * input, and set NULL values on the right.
                             */
                            for (int i = 0; i <leftTupleSize; i ++) {
                                outputTuple[i].copyFrom(leftTuple[i]);
                            }
                            for (int i = leftTupleSize; i < outputTuple.size();
                                 i ++) {
                                outputTuple[i].pData = NULL;
                            }
                            joinState = ProduceLeftOuter;
                            break;
                        }
                    }
                }
                break;
            }
        case ProduceInner:
            {
                /*
                 * Producing the results.
                 * Handle output overflow and quantum expiration in
                 * ProducePending.
                 */
                if (hashTableReader.getNext(rightTuple)) {
                    for (int i = 0; i <rightTupleSize; i ++) {
                        outputTuple[i + leftTupleSize].copyFrom(rightTuple[i]);
                    }
                    joinState = ProducePending;
                    /*
                     * Come back to this state after producing the output tuple
                     * successfully.
                     */
                    nextState = ProduceInner;
                } else {
                    leftReader.consumeTuple();
                    joinState = Probe;
                }
                break;
            }
        case ProduceLeftSemi:
            {
                /*
                 * Producing the results.
                 * Handle output overflow and quantum expiration in
                 * ProducePending.
                 */
                if (nextState == ProducePending) {
                    joinState = ProducePending;
                    /*
                     * Come back to this state after producing the output tuple
                     * successfully.
                     */
                    nextState = ProduceLeftSemi;
                } else {
                    leftReader.consumeTuple();
                    joinState = Probe;
                }
                break;
            }
        case ProduceLeftOuter:
            {
                if (pOutAccessor->produceTuple(outputTuple)) {
                    leftReader.consumeTuple();
                    numTuplesProduced++;
                    joinState = Probe;
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
        case ProduceRightOuter:
            {
                /*
                 * Producing the results.
                 * Handle output overflow and quantum expiration in
                 * ProducePending.
                 */
                if (hashTableReader.getNext(rightTuple)) {
                    for (int i = 0; i <rightTupleSize; i ++) {
                        outputTuple[i + leftTupleSize].copyFrom(rightTuple[i]);
                    }
                    joinState = ProducePending;
                    /*
                     * Come back to this state after producing the output tuple
                     * successfully.
                     */
                    nextState = ProduceRightOuter;
                } else {
                    /*
                     * Probing for this plan is done.
                     */
                    joinState = GetNextPlan;
                }
                break;
            }
        case ProduceRightAnti:
            {
                /*
                 * Producing the results.
                 * Handle output overflow and quantum expiration in
                 * ProducePending.
                 */
                if (hashTableReader.getNext(outputTuple)) {
                    joinState = ProducePending;
                    /*
                     * Come back to this state after producing the output tuple
                     * successfully.
                     */
                    nextState = ProduceRightAnti;
                } else {
                    /*
                     * Probing for this plan is done.
                     */
                    joinState = GetNextPlan;
                }
                break;
            }
        case ProducePending:
            {
                if (pOutAccessor->produceTuple(outputTuple)) {
                    numTuplesProduced++;
                    joinState = nextState;
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
                hashTable.releaseResources();
                return EXECRC_EOS;
            }
        }
    }
    
    /*
     * The state machine should never come here.
     */
    assert(false);
}

void LhxJoinExecStream::closeImpl()
{
    hashTable.releaseResources();
    rootPlan->close();
    ConfluenceExecStream::closeImpl();
}

void LhxJoinExecStream::setJoinType(
    LhxJoinExecStreamParams const &params)
{
    /*
     * just some shorthand
     */
    bool li = params.leftInner;
    bool lo = params.leftOuter;
    bool ri = params.rightInner;
    bool ro = params.rightOuter;

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
     * LeftInner LeftOuter RightInner RightOuter   JoinType
     *     F         F          F          T       Right Anti
     *     T         F          F          F       Left Semi
     *     T         F          T          F       Inner Join
     *     T         F          T          T       Right Outer
     *     T         T          T          F       Left Outer
     *     T         T          T          T       Full Outer       
     */    

    rightAnti  = (!li && !lo && !ri &&  ro);
    leftSemi   = ( li && !lo && !ri && !ro);
    innerJoin  = ( li && !lo &&  ri && !ro);
    rightOuter = ( li && !lo &&  ri &&  ro);
    leftOuter  = ( li &&  lo &&  ri && !ro);
    fullOuter  = ( li &&  lo &&  ri &&  ro);
    
    /*
     * By construction, at most one of the above six is true for a combination
     * of values.
     * Now make sure at least one of them is true.
     * Otherwise, the optimizer has passed in a join type not supported by this
     * join implementation.
     */
    assert (rightAnti || leftSemi || innerJoin || rightOuter || leftOuter ||
            fullOuter);

    regularJoin   = !params.setopDistinct && !params.setopAll;
    setopDistinct =  params.setopDistinct && !params.setopAll;
    setopAll      = !params.setopDistinct &&  params.setopAll;

    assert (!setopAll && (regularJoin || setopDistinct));    
}

FENNEL_END_CPPFILE("$Id$");

// End LhxJoinExecStream.cpp
