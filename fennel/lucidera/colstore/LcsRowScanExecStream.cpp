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
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsRowScanExecStream::prepare(LcsRowScanExecStreamParams const &params)
{
    LcsRowScanBaseExecStream::prepare(params);

    isFullScan = params.isFullScan;
    hasExtraFilter = params.hasExtraFilter;

    // Set up input if not full scan(which has no input)
    if (!isFullScan) {
        // setup tuple data for input stream
        ridTupleData.compute(inAccessors[0]->getTupleDesc());
    
        // validate input stream parameters
        TupleDescriptor inputDesc = inAccessors[0]->getTupleDesc();
        assert(inputDesc.size() == 3);
        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor expectedRidDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_RECORDNUM));
        assert(inputDesc[0] == expectedRidDesc);
    }

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
    if (isFullScan) {
        rid = LcsRid(0);
    } else {
        // only initialize LBmRidReader if not full scan.
        ridReader.init(inAccessors[0], ridTupleData);
    }
}

void LcsRowScanExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    LcsRowScanBaseExecStream::getResourceRequirements(minQuantity, optQuantity);
}

ExecStreamResult LcsRowScanExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (!isFullScan && inAccessors[0]->getState() == EXECBUF_EOS) {
        // Check for input EOS if not full table scan.
        // Full table scan does not have any input.
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {

        uint iClu;

        while (!producePending) {
            if (!isFullScan) {
                ExecStreamResult rc = ridReader.readRidAndAdvance(rid);
                if (rc == EXECRC_EOS) {
                    pOutAccessor->markEOS();
                    return rc;
                } else if (rc != EXECRC_YIELD) {
                    return rc;
                }
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
        
        if (isFullScan) {
            // if tuple not found, reached end of table,
            // else move to next rid
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
