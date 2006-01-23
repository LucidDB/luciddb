/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsRowScanExecStream::prepare(LcsRowScanExecStreamParams const &params)
{
    LcsRowScanBaseExecStream::prepare(params);

    inputTuple.compute(pInAccessor->getTupleDesc());

    // setup tuple data for input stream
    // for now, assume the input stream is a stream of rids; 
    // this will be changed to a bitmap later
    ridTupleData.compute(pInAccessor->getTupleDesc());

    assert(projDescriptor == pOutAccessor->getTupleDesc());
    pOutAccessor->setTupleShape(projDescriptor);

    outputTupleData.computeAndAllocate(projDescriptor);
}

void LcsRowScanExecStream::open(bool restart)
{
    LcsRowScanBaseExecStream::open(restart);
    producePending = false;
    tupleFound = false;
    nRidsRead = 0;
    fullTableScan = false;
}

void LcsRowScanExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    LcsRowScanBaseExecStream::getResourceRequirements(minQuantity, optQuantity);
}

ExecStreamResult LcsRowScanExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (!fullTableScan && pInAccessor->getState() == EXECBUF_EOS) {
        // if the input stream is empty, do a full table scan
        if (nRidsRead == 0) {
            fullTableScan = true;
            rid = LcsRid(0);
        } else {
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {

        uint iClu;

        while (!producePending) {
            if (!fullTableScan) {
                if (!pInAccessor->demandData()) {
                    return EXECRC_BUF_UNDERFLOW;
                }

                pInAccessor->unmarshalTuple(ridTupleData);
                rid = *reinterpret_cast<LcsRid const *> (ridTupleData[0].pData);
            }

            // Go through each cluster, forming rows and checking ranges

            uint prevClusterEnd = 0;
            for (iClu = 0; iClu <  nClusters; iClu++) {

                SharedLcsClusterReader &pScan = pClusters[iClu];

                // if we have not read a batch yet or we've reached the
                // end of a batch, position to the rid we want to read

                if (!pScan->isPositioned() || rid >= pScan->getRangeEndRid()) {
                    bool rc = pScan->position(rid);

                    // rid not found, so just consume the rid and 
                    // continue
                    if (rc == false)
                        break;

                    assert(rid >= pScan->getRangeStartRid()
                           && rid < pScan->getRangeEndRid());

                    // Tell all column scans that the batch has changed.
                    syncColumns(pScan);
                } else {
                    // Should not have moved into previous batch.
                    assert(rid > pScan->getRangeStartRid());

                    // move to correct position within scan; we know we
                    // will not fall off end of batch, so use non-checking
                    // function (for speed)
                    pScan->advanceWithinBatch(
                        opaqueToInt(rid - pScan->getCurrentRid()));
                }

                readColVals(pScan, outputTupleData, prevClusterEnd);
                prevClusterEnd += pScan->nColsToRead;
            }

            if (iClu == nClusters) {
                tupleFound = true;
            }
            producePending = true;
        }
            
        // produce tuple
        if (tupleFound && !pOutAccessor->produceTuple(outputTupleData)) {
            return EXECRC_BUF_OVERFLOW;
        }
        // reset datum pointers, in case of nulls
        outputTupleData.resetBuffer();
        producePending = false;
        
        if (!fullTableScan) {
            pInAccessor->consumeTuple();
        } else {

            // full table scan -- if tuple not found, reached end
            // of table, else move to next rid
            if (!tupleFound) {
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
            rid++;
        }

        tupleFound = false;
        nRidsRead++;
    }

    return EXECRC_QUANTUM_EXPIRED;
}

void LcsRowScanExecStream::closeImpl()
{
    LcsRowScanBaseExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End LcsRowScanExecStream.cpp
