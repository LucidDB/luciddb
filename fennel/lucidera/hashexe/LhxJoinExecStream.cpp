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

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxJoinExecStream::prepare(
    LhxJoinExecStreamParams const &params)
{
    leftInner  = params.leftInner;
    leftOuter  = params.leftOuter;
    rightInner = params.rightInner;
    rightOuter = params.rightOuter;

    /*
     * Returning matching rwos from both inputs.
     * Does not support semi or anti join currently.
     */
    assert (leftInner && rightInner);

    ConfluenceExecStream::prepare(params);

    joinInfo.streamBufAccessor[0] = inAccessors[0];
    joinInfo.streamBufAccessor[1] = inAccessors[1];
    
    joinInfo.inputDesc[0] = inAccessors[0]->getTupleDesc();
    joinInfo.inputDesc[1] = inAccessors[1]->getTupleDesc();

    TupleDescriptor &leftDesc  = (TupleDescriptor &)joinInfo.inputDesc[0];
    TupleDescriptor &rightDesc = (TupleDescriptor &)joinInfo.inputDesc[1];

    leftTuple.compute(leftDesc);
    rightTuple.compute(rightDesc);
    leftTupleSize = leftTuple.size();
    rightTupleSize = rightTuple.size();

    /*
     * Since null values do not match, filter null values if non-matching
     * tuples are not needed.
     */
    leftFilterNull  = !leftOuter;
    rightFilterNull = !rightOuter;

    int i, j;

    /*
     * Use up to 10000 slots, or 10 blocks, to store slots.
     */
    numSlotsHashTable = min(hashTable.slotsNeeded(params.cndKeys), (uint)10000);

    assert (params.leftKeyProj.size() == params.rightKeyProj.size());

    joinInfo.keyProj[0] = params.leftKeyProj;
    joinInfo.keyProj[1] = params.rightKeyProj;
    
    TupleProjection &leftKeyProj  = (TupleProjection &)joinInfo.keyProj[0];
    TupleProjection &rightKeyProj  = (TupleProjection &)joinInfo.keyProj[1];
    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;

    for (j = 0; j < leftKeyProj.size(); j ++) {
        keyDesc.push_back(leftDesc[leftKeyProj[j]]);
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
            joinInfo.dataProj.push_back(i);
        }
    }

    /* 
     * number of block required to perform the join, as given by the optimizer,
     * completely in memory.
     */
    uint usablePageSize = 
        (params.scratchAccessor.pSegment)->getUsablePageSize();

    /*
     * Cache pages requirement: put at 100000 blocks (or 400M for blocksize of 4K)
     */
    uint hashTableBlocks = 
        hashTable.blocksNeeded(
            params.numRows, params.cndKeys, 
            keyDesc, dataDesc, usablePageSize);

    numBlocksHashTable = 
        max((uint32_t)10000, hashTableBlocks + 10);
   
    TupleDescriptor outputDesc;

    if (params.outputProj.size() != 0) {
        outputDesc.projectFrom(params.outputTupleDesc, params.outputProj);
    } else {
        outputDesc = params.outputTupleDesc;
    }

    outputTuple.compute(outputDesc);    
    pOutAccessor->setTupleShape(outputDesc);

    joinInfo.memSegmentAccessor = params.scratchAccessor;
    joinInfo.externalSegmentAccessor.pCacheAccessor = params.pCacheAccessor;
    joinInfo.externalSegmentAccessor.pSegment = params.pTempSegment;
}

void LhxJoinExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConfluenceExecStream::getResourceRequirements(minQuantity,optQuantity);
    SharedCache pCache = (joinInfo.memSegmentAccessor.pCacheAccessor)->getCache();
    
    /*
     * Let hash table use at most 50% os the total cache size.
     */
    uint cacheLimit = pCache->getAllocatedPageCount() / 2;
    
    minQuantity.nCachePages += min(numBlocksHashTable, cacheLimit);

    optQuantity = minQuantity;
}

void LhxJoinExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    ConfluenceExecStream::setResourceAllocation(quantity);
    joinInfo.numCachePages = quantity.nCachePages;
}

void LhxJoinExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);

    if (restart) {
        hashTable.releaseResources();
    } else {
        uint partitionLevel = 0;
        hashTable.init(partitionLevel, joinInfo);
        hashTableReader.init(&hashTable, joinInfo);
    }

    bool status = hashTable.allocateResources(numSlotsHashTable);

    assert(status);

    /*
     * Create the root plan.
     *
     * The execute state machine operates at the plan level.
     */
    leftPart = SharedLhxPartition(new LhxPartition());
    rightPart = SharedLhxPartition(new LhxPartition());

    leftPart->firstPageId = NULL_PAGE_ID;
    leftPart->inputIndex = LeftInputIndex;

    rightPart->firstPageId = NULL_PAGE_ID;
    rightPart->inputIndex = RightInputIndex;

    rootPlan =  SharedLhxPlan(new LhxPlan());
    uint partitionLevel = 0;
    uint numChild       = 2;
    LhxPlan *parentPlan = NULL;
    rootPlan->init(partitionLevel, numChild, leftPart, rightPart, parentPlan);

    curPlan = rootPlan.get();
    isTopPartition = true;

    rightReader.open(curPlan->getPartition(RightInputIndex), joinInfo, true);
    joinState = Build;
}

ExecStreamResult LhxJoinExecStream::execute(ExecStreamQuantum const &quantum)
{
    TupleProjection &leftKeyProj  = (TupleProjection &)joinInfo.keyProj[0];
    TupleProjection &rightKeyProj = (TupleProjection &)joinInfo.keyProj[1];

	while (true)
	{
		switch (joinState)
        {
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
                            leftReader.open(curPlan->getPartition(0), joinInfo, true);                            
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
                     * When null values are filtered, and this tuple does
                     * contain null in its key columns, do not add to hash
                     * table.
                     */
                    if (!(rightFilterNull &&
                          rightTuple.containsNull(rightKeyProj))) {
                        if (!hashTable.addTuple(rightTuple)) {
                            /*
                             * If hash table is full, partition input data.
                             *
                             * First, partition the left.
                             */
                            partInfo.reset(true);
                            partInfo.init(LeftInputIndex,
                                curPlan->getPartition(LeftInputIndex),
                                curPlan->numChildPart, joinInfo);
                            joinState = Partition;
                            break;
                        };
                    }
                    rightReader.consumeTuple();
                }
                break;
            }
        case Partition:
            {
                for (;;) {
                    if (curPlan->generatePartitions(joinInfo, partInfo, false)
                        == PartitionUnderflow) {
                        /*
                         * Request more data from producer.
                         */
                        return EXECRC_BUF_UNDERFLOW;
                    } else {
                        if (partInfo.curInputIndex == LeftInputIndex) {
                            /*
                             * Now partition the right input.
                             * The in-memory hash table is also part of the
                             * partition.
                             */
                            partInfo.reset(false);
                            partInfo.init(RightInputIndex,
                                curPlan->getPartition(RightInputIndex),
                                curPlan->numChildPart, joinInfo,
                                &hashTableReader, &rightReader, rightTuple);
                        } else {
                            /*
                             * Finished building the partitions for both
                             * inputs.
                             */
                            break;
                        }
                    }
                }
                partInfo.reset(false);
                joinState = CreateChildPlan;
                break;
            }
        case CreateChildPlan:
            {
                FENNEL_TRACE(TRACE_FINE, (curPlan->dataTrace).str());

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

                hashTable.init(curPlan->partitionLevel, joinInfo);
                hashTableReader.init(&hashTable, joinInfo);

                bool status = hashTable.allocateResources(numSlotsHashTable);
                assert(status);
                rightReader.open(curPlan->getPartition(1), joinInfo, true);
                joinState = Build;
                break;                
            }
        case GetNextPlan:
            {
                curPlan = curPlan->getNextLeaf();
                if (curPlan) {
                    hashTable.releaseResources();

                    hashTable.init(curPlan->partitionLevel, joinInfo);
                    hashTableReader.init(&hashTable, joinInfo);

                    bool status = hashTable.allocateResources(numSlotsHashTable);
                    assert(status);
                    rightReader.open(curPlan->getPartition(1), joinInfo, true);
                    joinState = Build;
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
                            if (!rightOuter) {
                                /*
                                 * Probing for this plan is done.
                                 */
                                joinState = GetNextPlan;
                            } else {
                                /*
                                 * Set the output tuple to have NULL values on
                                 * the left, and get return all the
                                 * non-matching tuples in the hash table on the
                                 * right.
                                 */
                                for (int i = 0; i <leftTupleSize; i ++) {
                                    outputTuple[i].pData = NULL;
                                }

                                hashTableReader.bindUnMatched();
                                joinState = ProduceRightOuter;
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
                    bool isProbing = true;

                    /*
                     * Try to locate matching key in the hash table.
                     * If this tuple does contain null in its key columns, it
                     * will not join so hash table lookup is not needed.
                     */
                    if (!leftTuple.containsNull(leftKeyProj)) {
                        keyBuf = hashTable.findKey(leftTuple, leftKeyProj,
                            isProbing);
                    }
        
                    if (keyBuf) {
                        /*
                         * Set the output tuple to include only the left input,
                         * and get the next matching tuple from the right.
                         */
                        for (int i = 0; i <leftTupleSize; i ++) {
                            outputTuple[i].copyFrom(leftTuple[i]);
                        }
                        /**
                         * Output the joined tuple.
                         */
                        hashTableReader.bindKey(keyBuf);
                        joinState = ProduceInner;
                        break;
                    } else {
                        /*
                         * No match. Need to return the leftTuple if leftOuter
                         * join.
                         */
                        if (!leftOuter) {
                            leftReader.consumeTuple();
                        } else {
                            /*
                             * Set the output tuple to include only the left
                             * input, and set NULL values on the right.
                             */
                            for (int i = 0; i <leftTupleSize; i ++) {
                                outputTuple[i].copyFrom(leftTuple[i]);
                            }
                            for (int i = leftTupleSize; i < outputTuple.size(); i ++) {
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
                 * Handle output overflow and quantum expiration in ProducePending.
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
                 * Handle output overflow and quantum expiration in ProducePending.
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
    ConfluenceExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End LhxJoinExecStream.cpp
