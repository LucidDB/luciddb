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
#include "fennel/lucidera/colstore/LcsRowScanBaseExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LcsRowScanBaseExecStream::LcsRowScanBaseExecStream()
{
    nClusters = 0;
}

void LcsRowScanBaseExecStream::prepare(
    LcsRowScanBaseExecStreamParams const &params)
{
    ConfluenceExecStream::prepare(params);

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

    // setup projected tuple descriptor, by reshuffling allClusterTupleDesc
    // built above, into the correct projection order

    for (uint i = 0; i < newProj.size(); i++) {
        projDescriptor.push_back(allClusterTupleDesc[newProj[i]]);
    }

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
}

void LcsRowScanBaseExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);
    for (uint i = 0; i < nClusters; i++) {
        pClusters[i]->open();
    }
}

void LcsRowScanBaseExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConfluenceExecStream::getResourceRequirements(minQuantity, optQuantity);

    // 2 pages per cluster (not taking into account pre-fetches yet)
    // - 1 for cluster page
    // - 1 for btree page
    minQuantity.nCachePages += (nClusters * 2);
    
    optQuantity = minQuantity;
}

void LcsRowScanBaseExecStream::closeImpl()
{
    ConfluenceExecStream::closeImpl();
    for (uint i = 0; i < nClusters; i++) {
        pClusters[i]->close();
    }
}

void LcsRowScanBaseExecStream::syncColumns(SharedLcsClusterReader &pScan)
{
    for (uint iCluCol = 0; iCluCol < pScan->nColsToRead; iCluCol++) {
        pScan->clusterCols[iCluCol].sync();
    }
}

void LcsRowScanBaseExecStream::readColVals(
    SharedLcsClusterReader &pScan, TupleDataWithBuffer &tupleData,
    uint colStart)
{
    for (uint iCluCol = 0; iCluCol < pScan->nColsToRead; iCluCol++) {

        // Get value of each column and load it to the appropriate
        // tuple datum entry 
        PBuffer curValue = pScan->clusterCols[iCluCol].getCurrentValue();
        tupleData[projMap[colStart + iCluCol]].loadLcsDatum(curValue);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LcsRowScanBaseExecStream.cpp
