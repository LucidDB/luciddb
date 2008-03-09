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
    enableSubPartStat = params.enableSubPartStat;

    buildInputIndex = hashInfo.inputDesc.size() - 1;

    /* 
     * number of block and slots required to perform the aggregation in memory,
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
     * Set aside one cache block per child partition writer for I/O
     */
    uint numInputs = 1;
    numMiscCacheBlocks = LhxPlan::LhxChildPartCount * numInputs;
}

void LhxAggExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity,
    ExecStreamResourceSettingType &optType)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);
    
    uint minPages = 
        LhxHashTable::LhxHashTableMinPages * LhxPlan::LhxChildPartCount
        + numMiscCacheBlocks;
    minQuantity.nCachePages += minPages;
    // if valid stats weren't passed in, make an unbounded resource request
    if (isMAXU(numBlocksHashTable)) {
        optType = EXEC_RESOURCE_UNBOUNDED;
    } else {
        // make sure the opt is bigger than the min; otherwise, the
        // resource governor won't try to give it extra
        optQuantity.nCachePages += std::max(minPages + 1, numBlocksHashTable);
        optType = EXEC_RESOURCE_ESTIMATE;
    }
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

    // REVIEW jvs 25-Aug-2006: Fennel coding convention is pParentPlan,
    // pBuildPart, etc.  Same comment applies everywhere.  Also, consider using
    // boost::ptr_vector<LhxPartition> rather than
    // std::vector<SharedLhxPartition> (unless shared pointers are really
    // required).
    
    /*
     * Create the root plan.
     *
     * The execute state machine operates at the plan level.
     */
    vector<SharedLhxPartition> partitionList;

    buildPart = SharedLhxPartition(new LhxPartition());
    // REVIEW jvs 25-Aug-2006:  Why does buildPart->segStream need to be reset
    // immediately after construction?
    buildPart->segStream.reset();
    buildPart->inputIndex = 0;
    partitionList.push_back(buildPart);

    rootPlan = SharedLhxPlan(new LhxPlan());
    rootPlan->init(WeakLhxPlan(), partitionLevel, partitionList,
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
        // REVIEW jvs 25-Aug-2006:  Some compilers do better if you
        // put the most commonly used cases first in a switch.  Definitely
        // from a "follow-the-logic" standpoint, a testing-only state
        // like ForcePartitionBuild belongs last.
        switch (aggState)
        {
            // REVIEW jvs 25-Aug-2006:  I'm not sure that repeating all
            // of this code between the ForcePartitionBuild and Build
            // states is worth it just to remove one test from the
            // inner loop.
        case ForcePartitionBuild:
            {
                /*
                 * Build
                 */
                // REVIEW jvs 25-Aug-2006:  Is it really necessary to compute
                // the tuple every time through here?
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
                                 * Top level: request more data from stream
                                 * producer.
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
                                 * Top level: request more data from stream
                                 * producer.
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
                        // REVIEW jvs 25-Aug-2006:  only one input for agg..
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

                // REVIEW jvs 25-Aug-2006:  This comment makes it sound
                // like it's walking multiple levels in the plan tree
                // right here, but really it's just walking down to the
                // first leaf it just created (i.e. one step in
                // recursion if curPlan was already non-root).
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
                // REVIEW jvs 25-Aug-2006: Is there a reason tuples can't be
                // pumped out in a loop right here?  Popping in and out of
                // the state machine for every tuple is a bit of overhead.
                // It's only a couple of lines of code which would be
                // duplicated.  (An inline method would contradict
                // my earlier comment about numTuplesProduced being
                // a local variable.)
                /*
                 * Producing the results.  Handle output overflow and quantum
                 * expiration in ProducePending.
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
    // REVIEW jvs 25-Aug-2006: Are there other resources that ought to be
    // released here?  Anything in hashTableReader, partInfo, buildPart,
    // buildReader?  Or does that all get cleaned up implicitly?
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
     * [ group-by keys, aggregates ]
     */
    TupleDescriptor &hashDesc = hashInfo.inputDesc.back();

    /*
     * Fields corresponding to the aggregates in hashDesc
     */
    TupleProjection &aggsProj = hashInfo.aggsProj;

    /**
     * Change original agg computers to compute based on partial
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
            permFail("unknown aggregation function: "
                     << pInvocation->aggFunction);
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

    // empty projection : do not filter nulls
    TupleProjection filterNullKeyProj;
    hashInfo.filterNullKeyProj.push_back(filterNullKeyProj);

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
         * Hashing is special for varchar types (the trailing blanks are
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

    // REVIEW jvs 25-Aug-2006: It's possible to get rid of this nullability
    // type transformation (but it requires matching changes at the Farrago
    // level).  The reason is that LhxAggExecStream is only used for GROUP BY.
    // Since empty groups are only possible with full-table agg, they are not
    // an issue with GROUP BY.  So, the output can only be null if the input
    // admits nulls.  However, the validator currently applies the
    // transformation in all cases (e.g. SqlSumAggFunction uses
    // rtiFirstArgTypeForceNullable).  To do it right, it would need to be
    // context-sensitive (and SortedAggExecStream would need to be changed to
    // match, discriminating on whether any group keys were specified).
    // Probably not worth it.

    // REVIEW jvs 25-Aug-2006: What is the prevTupleDesc mentioned here?
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
