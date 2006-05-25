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

    hashInfo.streamBufAccessor.push_back(pInAccessor);

    TupleDescriptor inputDesc = pInAccessor->getTupleDesc();

    inputTuple.compute(inputDesc);
    
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

    uint usablePageSize = 
        (params.scratchAccessor.pSegment)->getUsablePageSize();

    /* 
     * number of block required to perform the join, as given by the 
     * optimizer, completely in memory.
     */
    uint hashTableBlocks = 
        hashTable.blocksNeeded(
            params.numRows, params.cndGroupByKeys, 
            keyDesc, dataDesc, usablePageSize);

    /*
     * Cache pages requirement: at least 10000 blocks
     * (or 40M for blocksize of 4K)
     */
    numBlocksHashTable = 
        max((uint32_t)10000, hashTableBlocks + 10);

    /*
     * Use between 0.1% and 1% of cache pages to store slots.
     */
    numSlotsHashTable =
        max(hashTable.slotsNeeded(params.cndGroupByKeys), (uint)10000);

    numSlotsHashTable = min(numSlotsHashTable, (uint)100000);
   
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
}

void LhxAggExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);
    SharedCache pCache = 
        (hashInfo.memSegmentAccessor.pCacheAccessor)->getCache();
    
    /*
     * Let hash table use at most 50% os the total cache size.
     */
    uint cacheLimit = pCache->getAllocatedPageCount() / 2;
    
    minQuantity.nCachePages += min(numBlocksHashTable, cacheLimit);

    optQuantity = minQuantity;
}

void LhxAggExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    ConduitExecStream::setResourceAllocation(quantity);
    hashInfo.numCachePages = quantity.nCachePages;
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

    bool status = hashTable.allocateResources(numSlotsHashTable);

    assert(status);

    /*
     * Create the root plan.
     *
     * The execute state machine operates at the plan level.
     */
    buildPart = SharedLhxPartition(new LhxPartition());

    buildPart->firstPageId = NULL_PAGE_ID;
    buildPart->inputIndex = 0;

    rootPlan =  SharedLhxPlan(new LhxPlan());
    uint numInput = 1;
    uint numChild       = 2;
    LhxPlan *parentPlan = NULL;
    vector<SharedLhxPartition> partitionList;
    partitionList.push_back(buildPart);

    rootPlan->init(partitionLevel, numChild, partitionList, parentPlan);

    curPlan = rootPlan.get();
    isTopPartition = true;

    buildReader.open(curPlan->getPartition(buildInputIndex), hashInfo, true);

    /*
     * Initialize recursive partitioning context.
     */
    partInfo.init(numInput, numChild, &hashInfo);

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
                        buildReader.unmarshalTuple(inputTuple);
                    }

                    /*
                     * Add tuple to hash table.
                     * When null values are filtered, and this tuple does
                     * contain null in its key columns, do not add to hash
                     * table.
                     */
                    if (!hashTable.addTuple(inputTuple)) {                        
                        partInfo.open(buildInputIndex,
                            curPlan->getPartition(buildInputIndex));
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
                 * now recursice down the plan tree to get the first leaf plan.
                 */
                curPlan = curPlan->getFirstChild().get();

                isTopPartition = false;
                hashTable.releaseResources();

                hashTable.init(curPlan->partitionLevel, hashInfo);
                hashTableReader.init(&hashTable, hashInfo);

                bool status = hashTable.allocateResources(numSlotsHashTable);
                assert(status);
                buildReader.open(curPlan->getPartition(buildInputIndex),
                    hashInfo, true);

                /*
                 * Reset the list of partitions to prepare for the next level
                 * of recursive partitioning.
                 */
                partInfo.reset();
                aggState = Build;
                break;                
            }
        case GetNextPlan:
            {
                curPlan = curPlan->getNextLeaf();
                if (curPlan) {
                    hashTable.releaseResources();

                    hashTable.init(curPlan->partitionLevel, hashInfo);
                    hashTableReader.init(&hashTable, hashInfo);

                    bool status = hashTable.allocateResources(numSlotsHashTable);
                    assert(status);
                    buildReader.open(curPlan->getPartition(buildInputIndex),
                        hashInfo, true);
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



FENNEL_END_CPPFILE("$Id$");

// End LhxAggExecStream.cpp
