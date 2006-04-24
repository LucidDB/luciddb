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
    ConfluenceExecStream::prepare(params);    
    TupleDescriptor leftDesc = inAccessors[0]->getTupleDesc();
    TupleDescriptor rightDesc = inAccessors[1]->getTupleDesc();
    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;

    leftTuple.compute(leftDesc);
    rightTuple.compute(rightDesc);

    /*
     * Since null values do not match, filter null values if non-matching
     * tuples are not needed.
     */
    leftFilterNull  = !params.leftOuter;
    rightFilterNull = !params.rightOuter;

    int i, j;

    assert (params.leftKeyProj.size() == params.rightKeyProj.size());

    leftKeyProj = params.leftKeyProj;
    rightKeyProj = params.rightKeyProj;
    
    for (i = 0, j = 0; i < rightDesc.size(); i ++) {
        if ((j < rightKeyProj.size()) && (rightKeyProj[j] == i)) {
            keyDesc.push_back(rightDesc[i]);
            j ++;
        } else {
            dataDesc.push_back(rightDesc[i]);
            dataProj.push_back(i);
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
    numBlocksHashTable = 
        max((uint32_t)10000,
            hashTable.blocksNeeded(
                params.numRows, params.cndKeys, 
                keyDesc, dataDesc, usablePageSize)
            + 10);

    /*
     * Default number of slots is 10000(using up to 10 blocks to store slots).
     */
    numSlotsHashTable =
        max(hashTable.slotsNeeded(params.cndKeys), (uint32_t)10000);
    
    TupleDescriptor outputDesc;
    TupleDescriptor tmpDesc;
    tmpDesc=leftDesc;
    tmpDesc.insert(tmpDesc.end(),rightDesc.begin(),rightDesc.end());
    
    if (params.outputProj.size() != 0) {
        outputDesc.projectFrom(tmpDesc, params.outputProj);
    }

    outputDesc = tmpDesc;
    outputTuple.compute(outputDesc);    
    pOutAccessor->setTupleShape(outputDesc);

    joinInfo.memSegmentAccessor = params.scratchAccessor;
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
    hashTable.init(joinInfo.memSegmentAccessor, joinInfo.numCachePages,
        0, inAccessors[1]->getTupleDesc(), rightKeyProj, aggsProj, dataProj);
    hashTableReader.init(&hashTable, inAccessors[1]->getTupleDesc(),
        rightKeyProj, aggsProj, dataProj);
}

void LhxJoinExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);

    if (restart) {
        hashTable.releaseResources();
    }
    
    bool status = hashTable.allocateResources(numSlotsHashTable);

    assert(status);
    joinState = Building;
}

ExecStreamResult LhxJoinExecStream::execute(ExecStreamQuantum const &quantum)
{
    SharedExecStreamBufAccessor leftBufAccessor = inAccessors[0];
    SharedExecStreamBufAccessor rightBufAccessor = inAccessors[1];
    uint numTuplesProduced;
    

	while (true)
	{
		switch (joinState)
        {
        case Building:
            {
                /*
                 * Build
                 */
                for (;;) {
                    if (!rightBufAccessor->isTupleConsumptionPending()) {
                        if (rightBufAccessor->getState() == EXECBUF_EOS) {
                            /*
                             * break out of this loop, and start probing.
                             */
                            joinState = Probing;
                            numTuplesProduced = 0;
                            break;
                        }
                        if (!rightBufAccessor->demandData()) {
                            return EXECRC_BUF_UNDERFLOW;
                        }
                        rightBufAccessor->unmarshalTuple(rightTuple);
                    }

                    /*
                     * Add tuple to hash table.
                     * When null values are filtered, and this tuple does
                     * contain null in its key columns, do not add to hash
                     * table.
                     */
                    if (!(rightFilterNull &&
                          rightTuple.containsNull(rightKeyProj))) {
                        hashTable.addTuple(rightTuple, rightKeyProj, aggsProj,
                            dataProj);
                    }

                    rightBufAccessor->consumeTuple();
                }
                break;
            }
        case Probing:
            {
                /*
                 * Probe
                 */
                for (;;) {
                    if (!leftBufAccessor->isTupleConsumptionPending()) {
                        if (leftBufAccessor->getState() == EXECBUF_EOS) {
                            /*
                             * Probing is done
                             */
                            joinState = Done;
                            pOutAccessor->markEOS();
                        }
                        if (!leftBufAccessor->demandData()) {
                            return EXECRC_BUF_UNDERFLOW;
                        }
                        leftBufAccessor->unmarshalTuple(leftTuple);
                    }

                    PBuffer keyBuf = NULL;
                    
                    /*
                     * Try to locate matching key in the hash table.
                     * When null values are filtered, and this tuple does
                     * contain null in its key columns, it will not join so
                     * hash table lookup is not needed.
                     */
                    if (!(leftFilterNull &&
                          leftTuple.containsNull(leftKeyProj))) {
                        keyBuf = hashTable.findKey(leftTuple, leftKeyProj);
                    }
        
                    if (keyBuf) {
                        /**
                         * Half of output tuple.
                         */
                        outputTuple = leftTuple;
            
                        /**
                         * Output the joined tuple.
                         */
                        hashTableReader.bindKey(keyBuf);
                        joinState = Producing;
                        break;
                    } else {
                        leftBufAccessor->consumeTuple();
                    }
                }
                break;
            }
        case Producing:
            {
                /*
                 * Producing the results.
                 * Handle output overflow and quantum expiration in this state.
                 */
                for(;;) {
                    if (hashTableReader.getNext(rightTuple)) {
                        outputTuple.insert(outputTuple.end(),
                            rightTuple.begin(),rightTuple.end());
                        joinState = ProducePending;
                        break;
                    } else {
                        leftBufAccessor->consumeTuple();
                        joinState = Probing;
                        break;
                    }
                }
                break;
            }
        case ProducePending:
            {
                if (pOutAccessor->produceTuple(outputTuple)) {
                    /*
                     * Reset the left side.
                     */
                    outputTuple = leftTuple;
                    joinState = Producing;
                    numTuplesProduced++;
                } else {
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
