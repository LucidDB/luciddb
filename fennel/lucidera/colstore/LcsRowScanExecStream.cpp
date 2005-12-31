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
#include "fennel/btree/BTreeReader.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsRowScanExecStream::prepare(LcsRowScanExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    inputTuple.compute(pInAccessor->getTupleDesc());

    // Copy cluster definition parameters and setup btree readers for each
    // cluster.  Also, setup the full output tuple based on the ordered
    // list of cluster descriptors.

    nClusters = params.lcsClusterScanDefs.size();
    pClusters.reset(new SharedLcsClusterReader[nClusters]);

    uint clusterStart = 0;
    uint projCount = 0;
    TupleDescriptor allClusterTupleDesc;
    TupleProjection newProj;

    newProj.resize(params.outputProj.size());
    for (uint i = 0; i < nClusters; i++) {

        SharedLcsClusterReader &pClu = pClusters[i];

        BTreeExecStreamParams const &bTreeParams = params.lcsClusterScanDefs[i];

        BTreeDescriptor treeDescriptor;
        treeDescriptor.segmentAccessor.pSegment = bTreeParams.pSegment;
        treeDescriptor.segmentAccessor.pCacheAccessor =
            bTreeParams.pCacheAccessor;
        treeDescriptor.tupleDescriptor = bTreeParams.tupleDesc;
        treeDescriptor.keyProjection = bTreeParams.keyProj;
        treeDescriptor.rootPageId = bTreeParams.rootPageId;
        treeDescriptor.segmentId = bTreeParams.segmentId;
        treeDescriptor.pageOwnerId = bTreeParams.pageOwnerId;

        pClu = SharedLcsClusterReader(new LcsClusterReader(treeDescriptor));

        // setup the cluster and column readers to only read the columns
        // that are going to be projected
        uint clusterEnd = clusterStart +
            params.lcsClusterScanDefs[i].clusterTupleDesc.size() - 1;

        // create a vector of the columns that are projected from
        // this cluster and recompute the projection list
        // based on the individual cluster projections
        TupleProjection clusterProj;
        for (uint j = 0; j < newProj.size(); j++) {
            if (params.outputProj[j] >= clusterStart &&
                params.outputProj[j] <= clusterEnd)
            {
                clusterProj.push_back(params.outputProj[j] - clusterStart);
                newProj[j] = projCount++;
            }
        }
        clusterStart = clusterEnd + 1;

        // need to select at least one column from cluster;
        // otherwise, there's a bug in the optimizer
        assert(clusterProj.size() > 0);
        pClu->initColumnReaders(
            params.lcsClusterScanDefs[i].clusterTupleDesc.size(), clusterProj);
        for (uint j = 0; j < pClu->nColsToRead; j++) {
            allClusterTupleDesc.push_back(
                params.lcsClusterScanDefs[i].clusterTupleDesc[clusterProj[j]]);
        }
    }

    // setup tuple data for input stream
    // for now, assume the input stream is a stream of rids; 
    // this will be changed to a bitmap later
    ridTupleData.compute(pInAccessor->getTupleDesc());

    // setup projected output tuple, by reshuffling allClusterTupleDesc
    // built above, into the correct projection order

    TupleDescriptor projDescriptor;
    for (uint i = 0; i < newProj.size(); i++) {
        projDescriptor.push_back(allClusterTupleDesc[newProj[i]]);
    }
    pOutAccessor->setTupleShape(projDescriptor);

    // create a projection map to map cluster data read to the output
    // projection
    projMap.resize(newProj.size());
    for (uint i = 0; i < projMap.size(); i++) {
        for (uint j = 0; j < newProj.size(); j++) {
            if (newProj[j] == i) {
                projMap[i] = j;
            }
        }
    }

    outputTupleData.computeAndAllocate(projDescriptor);
}

void LcsRowScanExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    producePending = false;
    tupleFound = false;
    nRidsRead = 0;
    fullTableScan = false;
    for (uint i = 0; i < nClusters; i++) {
        pClusters[i]->open();
    }
}

void LcsRowScanExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // 2 pages per cluster (not taking into account pre-fetches yet)
    // - 1 for cluster page
    // - 1 for btree page
    minQuantity.nCachePages += (nClusters * 2);
    
    optQuantity = minQuantity;
}

ExecStreamResult LcsRowScanExecStream::execute(ExecStreamQuantum const &quantum)
{
    uint iClu;
    uint iCluCol;

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
                    for (iCluCol = 0; iCluCol < pScan->nColsToRead; iCluCol++) {
                        LcsColumnReader *pColScan = &pScan->clusterCols[iCluCol];

                        // synchronize column scan
                        pColScan->sync();
                    }
                } else {
                    // Should not have moved into previous batch.
                    assert(rid > pScan->getRangeStartRid());

                    // move to correct position within scan; we know we
                    // will not fall off end of batch, so use non-checking
                    // function (for speed)
                    pScan->advanceWithinBatch(opaqueToInt(rid) -
                        opaqueToInt(pScan->getCurrentRid()));
                }

                for (iCluCol = 0; iCluCol < pScan->nColsToRead; iCluCol++) {

                    LcsColumnReader *pColScan = &pScan->clusterCols[iCluCol];
                    PBuffer curValue;

                    // Get value of each column and load it to the appropriate
                    // tuple datum entry 

                    curValue = pColScan->getCurrentValue();
                    outputTupleData[projMap[prevClusterEnd + iCluCol]].
                        loadLcsDatum(curValue);
                }

                prevClusterEnd += pScan->nColsToRead;
            }

            if (iClu == nClusters) {
                tupleFound = true;
                // reset datum pointers, in case of nulls
                outputTupleData.resetBuffer();
            }
            producePending = true;
        }
            
        // produce tuple
        if (tupleFound && !pOutAccessor->produceTuple(outputTupleData)) {
            return EXECRC_BUF_OVERFLOW;
        }
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
    ConduitExecStream::closeImpl();
    for (uint i = 0; i < nClusters; i++) {
        pClusters[i]->close();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LcsRowScanExecStream.cpp
