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
#include "fennel/lucidera/hashexe/LhxAggExecStream.h"
#include "fennel/segment/Segment.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxAggExecStream::prepare(
    LhxAggExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    hashInfo.filterNull = false;
    hashInfo.removeDuplicate = false;

    hashInfo.streamBufAccessor.push_back(pInAccessor);

    TupleDescriptor inputDesc = pInAccessor->getTupleDesc();

    TupleDescriptor keyDesc;
    TupleProjection keyProj;
    TupleDescriptor dataDesc;
    vector<bool> isKeyColVarChar;
    uint i;

    groupByKeyCount = params.groupByKeyCount;
    for (i = 0; i < groupByKeyCount; i ++) {
        keyProj.push_back(i);
        keyDesc.push_back(inputDesc[i]);
    }
    
    hashInfo.keyProj.push_back(keyProj);

    // Attribute descriptor for COUNT output
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor countDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    /*
      Compute the accumulator result portion of prevTupleDesc based on
      requested aggregate function invocations, and instantiate polymorphic
      AggComputers bound to correct inputs.
    */
    for (AggInvocationConstIter pInvocation(params.aggInvocations.begin());
         pInvocation != params.aggInvocations.end();
         ++pInvocation)
    {
        switch(pInvocation->aggFunction) {
        case AGG_FUNC_COUNT:
            keyDesc.push_back(countDesc);
            break;
        case AGG_FUNC_SUM:
        case AGG_FUNC_MIN:
        case AGG_FUNC_MAX:
            // Key type is same as input type, but nullable
            keyDesc.push_back(inputDesc[pInvocation->iInputAttr]);
            keyDesc.back().isNullable = true;
            break;
        }
        TupleAttributeDescriptor const *pInputAttr = NULL;
        if (pInvocation->iInputAttr != -1) {
            pInputAttr = &(inputDesc[pInvocation->iInputAttr]);
        }
        aggComputers.push_back(
            AggComputer::newAggComputer(
                pInvocation->aggFunction,
                pInputAttr));
        aggComputers.back().setInputAttrIndex(pInvocation->iInputAttr);
        hashInfo.aggsProj.push_back(i++);
    }
    
    getPartialAggComputers(partialAggComputers, params.aggInvocations,
        keyDesc, hashInfo.aggsProj);

    for (i = 0; i < groupByKeyCount; i ++) {        
        /*
         * Hashing is special for varchar types(the trailing blanks are
         * insignificant).
         */
        if (inputDesc[i].pTypeDescriptor->getOrdinal()
            == STANDARD_TYPE_VARCHAR) {
            isKeyColVarChar.push_back(true);
        } else {
            isKeyColVarChar.push_back(false);
        }
    }

    hashInfo.inputDesc.push_back(keyDesc);
    buildInputIndex = hashInfo.inputDesc.size() - 1;

    hashInfo.isKeyColVarChar.push_back(isKeyColVarChar);

    /*
     * Let hash table use at most 50% os the total cache size.
     */
    uint cacheLimit, usablePageSize;

    cacheLimit =
        (params.scratchAccessor.pCacheAccessor)->getCache()
        ->getMaxLockedPages() / 2;

    usablePageSize = 
        (params.scratchAccessor.pSegment)->getUsablePageSize();

    /* 
     * number of block and slots required to perform the join in memory,
     * using estimates from the optimizer.
     */
    hashTable.calculateSize(
        params.numRows, params.cndGroupByKeys, 
        keyDesc, dataDesc, usablePageSize, cacheLimit,
        numBlocksHashTable);
    
    TupleDescriptor outputDesc;

    outputDesc = keyDesc;

    if (!params.outputTupleDesc.empty()) {
        assert (outputDesc == params.outputTupleDesc);
    }

    outputTuple.compute(outputDesc);    
    pOutAccessor->setTupleShape(outputDesc);

    hashInfo.memSegmentAccessor = params.scratchAccessor;
    hashInfo.externalSegmentAccessor.pCacheAccessor = params.pCacheAccessor;
    hashInfo.externalSegmentAccessor.pSegment = params.pTempSegment;
    hashInfo.cndKeys = params.cndGroupByKeys;

    /*
     * Set aside 10 cache blocks for I/O.
     */
    numMiscCacheBlocks = 10;
}

void LhxAggExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);
    
    minQuantity.nCachePages += numBlocksHashTable + numMiscCacheBlocks;

    optQuantity = minQuantity;
}

void LhxAggExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    ConduitExecStream::setResourceAllocation(quantity);
    hashInfo.numCachePages = quantity.nCachePages - numMiscCacheBlocks;
}


void LhxAggExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);

    if (restart) {
        hashTable.releaseResources();
    }

    uint partitionLevel = 0;
    hashTable.init(partitionLevel, hashInfo, &aggComputers);
    hashTableReader.init(&hashTable, hashInfo);

    bool status = hashTable.allocateResources();

    assert(status);

    /*
     * Create the root plan.
     *
     * The execute state machine operates at the plan level.
     */
    uint numInput = 1;
    uint numChild = 2;
    LhxPlan *parentPlan = NULL;
    vector<SharedLhxPartition> partitionList;

    buildPart = SharedLhxPartition(new LhxPartition());
    (buildPart->segStream).reset();
    buildPart->inputIndex = 0;

    partitionList.push_back(buildPart);

    rootPlan =  SharedLhxPlan(new LhxPlan());
    rootPlan->init(partitionLevel, numChild, partitionList, parentPlan);

    /*
     * Initialize recursive partitioning context.
     */
    isTopPartition = true;
    partInfo.init(numInput, numChild, &hashInfo);

    /*
     * Now starts at the first (root) plan.
     */
    curPlan = rootPlan.get();
    buildReader.open(curPlan->getPartition(buildInputIndex), hashInfo);
    aggState = Build;
}

ExecStreamResult LhxAggExecStream::execute(ExecStreamQuantum const &quantum)
{
    while (true)
    {
        switch (aggState)
        {
        case Build:
            {
                /*
                 * Build
                 */
                inputTuple.compute(buildReader.getTupleDesc());
                for (;;) {
                    if (!buildReader.isTupleConsumptionPending()) {
                        if (buildReader.getState() == EXECBUF_EOS) {
                            numTuplesProduced = 0;
                            /*
                             * break out of this loop, and start returning.
                             */
                            aggState = Produce;
                            break;
                        }

                        if (!buildReader.demandData()) {
                            if (isTopPartition) {
                                /*
                                 * Top level: request more data from stream producer.
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
                        buildReader.unmarshalTuple(inputTuple);
                    }

                    /*
                     * Add tuple to hash table.
                     */
                    if (!hashTable.addTuple(inputTuple)) {
                        if (isTopPartition) {
                            partInfo.open(buildInputIndex,
                                &hashTableReader, &buildReader, inputTuple, &aggComputers);
                        } else {
                            partInfo.open(buildInputIndex,
                                &hashTableReader, &buildReader, inputTuple, &partialAggComputers);
                        }
                        aggState = Partition;
                        break;
                    }
                    buildReader.consumeTuple();
                }
                break;
            }
        case Partition:
            {
                for (;;) {
                    if (curPlan->generatePartitions(hashInfo, partInfo, false)
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
                partInfo.close(buildInputIndex);
                aggState = CreateChildPlan;
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
                 * Reset the list of partitions to prepare for the next level
                 * of recursive partitioning.
                 */
                partInfo.reset();

                /*
                 * now recurse down the plan tree to get the first leaf plan.
                 */
                curPlan = curPlan->getFirstChild().get();

                isTopPartition = false;
                hashTable.releaseResources();

                /*
                 * At recursive level, the input tuple shape is
                 * different. Inputs are all partial aggregates now.
                 * Hash table needs to aggregate partial results using the
                 * correct agg computer interface.
                 */
                hashTable.init(curPlan->partitionLevel, hashInfo, &partialAggComputers);
                hashTableReader.init(&hashTable, hashInfo);

                bool status = hashTable.allocateResources();
                assert(status);

                buildReader.open(
                    curPlan->getPartition(buildInputIndex),
                    hashInfo);

                aggState = Build;
                break;                
            }
        case GetNextPlan:
            {
                curPlan = curPlan->getNextLeaf();
                if (curPlan) {
                    hashTable.releaseResources();

                    /*
                     * At recursive level, the input tuple shape is
                     * different. Inputs are all partial aggregates now.
                     * Hash table needs to aggregate partial results using the
                     * correct agg computer interface.
                     */
                    hashTable.init(curPlan->partitionLevel, hashInfo, &partialAggComputers);
                    hashTableReader.init(&hashTable, hashInfo);
                    bool status = hashTable.allocateResources();
                    assert(status);

                    buildReader.open(
                        curPlan->getPartition(buildInputIndex),
                        hashInfo);

                    aggState = Build;
                } else {
                    aggState = Done;
                }
                break;
            }
        case Produce:
            {
                /*
                 * Producing the results.
                 * Handle output overflow and quantum expiration in ProducePending.
                 */
                if (hashTableReader.getNext(outputTuple)) {
                    aggState = ProducePending;
                    /*
                     * Come back to this state after producing the output tuple
                     * successfully.
                     */
                    nextState = Produce;
                } else {
                    aggState = GetNextPlan;
                }
                break;
            }
        case ProducePending:
            {
                if (pOutAccessor->produceTuple(outputTuple)) {
                    numTuplesProduced++;
                    aggState = nextState;
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

void LhxAggExecStream::closeImpl()
{
    hashTable.releaseResources();
    ConduitExecStream::closeImpl();
}

void LhxAggExecStream::getPartialAggComputers(
    AggComputerList &partialAggComputers,
    AggInvocationList const &aggInvocations,
    TupleDescriptor const &keyDesc,
    TupleProjection const &aggsProj)
{
    AggFunction partialAggFunction;
    uint i = 0;

    assert (aggInvocations.size() == aggsProj.size());

    for (AggInvocationConstIter pInvocation(aggInvocations.begin());
         pInvocation != aggInvocations.end();
         ++pInvocation)
    {
        switch(pInvocation->aggFunction) {
        case AGG_FUNC_COUNT:
            partialAggFunction = AGG_FUNC_SUM;
            break;
        case AGG_FUNC_SUM:
        case AGG_FUNC_MIN:
        case AGG_FUNC_MAX:
            partialAggFunction = pInvocation->aggFunction;
            break;
        default:
            assert(false);
            partialAggFunction = pInvocation->aggFunction;
            break;
        }
        TupleAttributeDescriptor const *pInputAttr =
            &(keyDesc[aggsProj[i]]);

        partialAggComputers.push_back(
            AggComputer::newAggComputer(
                partialAggFunction,
                pInputAttr));
        partialAggComputers.back().setInputAttrIndex(aggsProj[i]);
        i ++;
    }    
}


FENNEL_END_CPPFILE("$Id$");

// End LhxAggExecStream.cpp
