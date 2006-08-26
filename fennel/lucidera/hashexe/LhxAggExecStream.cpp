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
    
    setHashInfo(params);
    setAggComputers(hashInfo, params.aggInvocations);

    /*
     * Force partitioning level. Only set in tests.
     */
    forcePartitionLevel = params.forcePartitionLevel;
    enableSubPartStat   = params.enableSubPartStat;

    buildInputIndex = hashInfo.inputDesc.size() - 1;

    /* 
     * number of block and slots required to perform the aggregatin in memory,
     * using estimates from the optimizer.
     */
    hashTable.calculateSize(hashInfo, buildInputIndex, numBlocksHashTable);
  
    TupleDescriptor outputDesc;

    outputDesc = hashInfo.inputDesc[buildInputIndex];

    if (!params.outputTupleDesc.empty()) {
        assert (outputDesc == params.outputTupleDesc);
    }

    outputTuple.compute(outputDesc);    
    pOutAccessor->setTupleShape(outputDesc);

    /*
     * Set aside 1 cache block per child partition writer for I/O.
     */
    uint numInputs = 1;
    numMiscCacheBlocks = LhxPlan::LhxChildPartCount * numInputs;
}

void LhxAggExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);
    
    minQuantity.nCachePages += 
        LhxHashTable::LhxHashTableMinPages * LhxPlan::LhxChildPartCount
        + numMiscCacheBlocks;
    optQuantity.nCachePages += numBlocksHashTable + numMiscCacheBlocks;
    /*
     * TODO(rchen 2006-08-08): use the real min above.
     */
    minQuantity.nCachePages = optQuantity.nCachePages;
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
    hashTable.init(partitionLevel, hashInfo, &aggComputers, buildInputIndex);
    hashTableReader.init(&hashTable, hashInfo, buildInputIndex);

    bool status = hashTable.allocateResources();

    assert(status);

    /*
     * Create the root plan.
     *
     * The execute state machine operates at the plan level.
     */
    LhxPlan *parentPlan = NULL;
    vector<SharedLhxPartition> partitionList;

    buildPart = SharedLhxPartition(new LhxPartition());
    (buildPart->segStream).reset();
    buildPart->inputIndex = 0;
    partitionList.push_back(buildPart);

    rootPlan = SharedLhxPlan(new LhxPlan());
    rootPlan->init(parentPlan, partitionLevel, partitionList,
        enableSubPartStat);

    /*
     * initialize recursive partitioning context.
     */
    partInfo.init(&hashInfo);

    /*
     * Now starts at the first (root) plan.
     */
    curPlan = rootPlan.get();
    isTopPlan = true;

    buildReader.open(curPlan->getPartition(buildInputIndex), hashInfo);

    aggState = (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
}

ExecStreamResult LhxAggExecStream::execute(ExecStreamQuantum const &quantum)
{
    while (true)
    {
        switch (aggState)
        {
        case ForcePartitionBuild:
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
                            if (isTopPlan) {
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
                     *
                     * NOTE: This is a testing state. Always partition up to
                     * forcePartitionLevel.
                     */
                    if (curPlan->getPartitionLevel() < forcePartitionLevel ||
                        !hashTable.addTuple(inputTuple)) {
                        if (isTopPlan) {
                            partInfo.open(&hashTableReader,
                                          &buildReader, inputTuple,
                                          &aggComputers);
                        } else {
                            partInfo.open(&hashTableReader,
                                          &buildReader, inputTuple,
                                          &partialAggComputers);
                        }
                        aggState = Partition;
                        break;
                    }
                    buildReader.consumeTuple();
                }
                break;
            }
        case Build:
            {
                /*
                 * Build
                 */
                inputTuple.compute(buildReader.getTupleDesc());
                for (;;) {
                    if (!buildReader.isTupleConsumptionPending()) {
                        if (buildReader.getState() == EXECBUF_EOS) {
                            buildReader.close();
                            numTuplesProduced = 0;
                            /*
                             * break out of this loop, and start returning.
                             */
                            aggState = Produce;
                            break;
                        }

                        if (!buildReader.demandData()) {
                            if (isTopPlan) {
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
                        if (isTopPlan) {
                            partInfo.open(&hashTableReader,
                                          &buildReader, inputTuple,
                                          &aggComputers);
                        } else {
                            partInfo.open(&hashTableReader,
                                          &buildReader, inputTuple,
                                          &partialAggComputers);
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
                aggState = CreateChildPlan;
                break;
            }
        case CreateChildPlan:
            {
                /*
                 * Link the newly created partitioned in the plan tree.
                 */
                curPlan->createChildren(partInfo, false, false);
                
                FENNEL_TRACE(TRACE_FINE, curPlan->toString());

                /*
                 * now recurse down the plan tree to get the first leaf plan.
                 */
                curPlan = curPlan->getFirstChild().get();
                isTopPlan = false;

                hashTable.releaseResources();

                /*
                 * At recursive level, the input tuple shape is
                 * different. Inputs are all partial aggregates now.
                 * Hash table needs to aggregate partial results using the
                 * correct agg computer interface.
                 */
                hashTable.init(curPlan->getPartitionLevel(), hashInfo,
                    &partialAggComputers, buildInputIndex);
                hashTableReader.init(&hashTable, hashInfo, buildInputIndex);

                bool status = hashTable.allocateResources();
                assert(status);

                buildReader.open(curPlan->getPartition(buildInputIndex),
                    hashInfo);

                aggState =
                    (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
                break;                
            }
        case GetNextPlan:
            {
                hashTable.releaseResources();
                curPlan = curPlan->getNextLeaf();

                if (curPlan) {
                    /*
                     * At recursive level, the input tuple shape is
                     * different. Inputs are all partial aggregates now.
                     * Hash table needs to aggregate partial results using the
                     * correct agg computer interface.
                     */
                    hashTable.init(curPlan->getPartitionLevel(), hashInfo,
                        &partialAggComputers, buildInputIndex);
                    hashTableReader.init(&hashTable, hashInfo, buildInputIndex);
                    bool status = hashTable.allocateResources();
                    assert(status);

                    buildReader.open(curPlan->getPartition(buildInputIndex),
                        hashInfo);

                    aggState = 
                        (forcePartitionLevel > 0) ? ForcePartitionBuild : Build;
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
    if (rootPlan) {
        rootPlan->close();
        rootPlan.reset();
    }
    ConduitExecStream::closeImpl();
}

void LhxAggExecStream::setAggComputers(
    LhxHashInfo &hashInfo,    
    AggInvocationList const &aggInvocations)
{
    /*
     * InputDesc from underlying producer.
     */
    TupleDescriptor inputDesc = pInAccessor->getTupleDesc();

    /*
     * TupleDescriptor used by the hash table, of the format:
     * [ grou-by keys, aggregates]
     */
    TupleDescriptor &hashDesc = hashInfo.inputDesc.back();

    /*
     * Fields corresponding to the aggregates in hashDesc
     */
    TupleProjection &aggsProj = hashInfo.aggsProj;

    /**
     * Change oroginal agg computers to compute based on partial
     * aggregates.
     */
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
        case AGG_FUNC_SINGLE_VALUE:
            partialAggFunction = pInvocation->aggFunction;
            break;
        default:
            assert(false);
            partialAggFunction = pInvocation->aggFunction;
            break;
        }

        /*
         * Add to aggregate computer list.
         */
        TupleAttributeDescriptor const *pInputAttr = NULL;
        if (pInvocation->iInputAttr != -1) {
            pInputAttr = &(inputDesc[pInvocation->iInputAttr]);
        }
        aggComputers.push_back(
            AggComputer::newAggComputer(
                pInvocation->aggFunction, pInputAttr));
        aggComputers.back().setInputAttrIndex(pInvocation->iInputAttr);

        /*
         * Add to partial aggregate computer list.
         */
        TupleAttributeDescriptor const *pInputAttrPartialAgg =
            &(hashDesc[aggsProj[i]]);
        partialAggComputers.push_back(
            AggComputer::newAggComputer(
                partialAggFunction, pInputAttrPartialAgg));
        partialAggComputers.back().setInputAttrIndex(aggsProj[i]);
        i ++;
    }
}

void LhxAggExecStream::setHashInfo(
    LhxAggExecStreamParams const &params)
{
    TupleDescriptor inputDesc = pInAccessor->getTupleDesc();

    hashInfo.streamBufAccessor.push_back(pInAccessor);

    hashInfo.cndKeys.push_back(params.cndGroupByKeys);
    hashInfo.numRows.push_back(params.numRows);

    hashInfo.filterNull.push_back(false);
    hashInfo.removeDuplicate.push_back(false);
    hashInfo.useJoinFilter.push_back(false);

    hashInfo.memSegmentAccessor = params.scratchAccessor;
    hashInfo.externalSegmentAccessor.pCacheAccessor = params.pCacheAccessor;
    hashInfo.externalSegmentAccessor.pSegment = params.pTempSegment;

    TupleProjection keyProj;
    vector<bool> isKeyColVarChar;

    for (int i = 0; i < params.groupByKeyCount; i ++) {
        keyProj.push_back(i);
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
    hashInfo.keyProj.push_back(keyProj);    
    hashInfo.isKeyColVarChar.push_back(isKeyColVarChar);

    /*
     * Empty data projection.
     */
    TupleProjection dataProj;
    hashInfo.dataProj.push_back(dataProj);

    /*
     * Set up keyDesc
     */
    TupleDescriptor keyDesc;
    keyDesc.projectFrom(inputDesc, keyProj);

    /*
     * Attribute descriptor for COUNT output
     */
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor countDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    /*
      Compute the accumulator result portion of prevTupleDesc based on
      requested aggregate function invocations, and instantiate polymorphic
      AggComputers bound to correct inputs.
    */
    int i = params.groupByKeyCount;
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
        case AGG_FUNC_SINGLE_VALUE:
            // Key type is same as input type, but nullable
            keyDesc.push_back(inputDesc[pInvocation->iInputAttr]);
            keyDesc.back().isNullable = true;
            break;
        }
        hashInfo.aggsProj.push_back(i++);
    }

    hashInfo.inputDesc.push_back(keyDesc);
}

FENNEL_END_CPPFILE("$Id$");

// End LhxAggExecStream.cpp
