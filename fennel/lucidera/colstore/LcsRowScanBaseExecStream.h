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

#ifndef Fennel_LcsRowScanBaseExecStream_Included
#define Fennel_LcsRowScanBaseExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/lucidera/colstore/LcsClusterReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Represents a single cluster in a table cluster scan
 */
struct LcsClusterScanDef : public BTreeExecStreamParams
{
    /**
     * Tuple descriptor of columns that make up the cluster
     */
    TupleDescriptor clusterTupleDesc;
};

typedef std::vector<LcsClusterScanDef> LcsClusterScanDefList;

/**
 * Indicates the clustered indexes that need to be read to scan a table and
 * the columns from the clusters that need to be projected in the scan result.
 */
struct LcsRowScanBaseExecStreamParams : public ConduitExecStreamParams
{
    /**
     * Ordered list of cluster scans
     */
    LcsClusterScanDefList lcsClusterScanDefs;

    /**
     * projection from scan
     */
    TupleProjection outputProj;
};

/**
 * Implements basic elements required to scan clusters in an exec stream
 */
class LcsRowScanBaseExecStream : public ConduitExecStream
{
protected:
    /**
     * Projection map that maps columns read from cluster to their position
     * in the output projection
     */
    std::vector<uint> projMap;

    /**
     * Number of clusters to be scanned
     */
    uint nClusters;

    /**
     * Array containing cluster readers
     */
    boost::scoped_array<SharedLcsClusterReader> pClusters;

    /**
     * Tuple descriptor representing columns to be projected from scans
     */
    TupleDescriptor projDescriptor;

    /**
     * Positions column readers based on new cluster reader position
     *
     * @param pScan cluster reader
     */
    void syncColumns(SharedLcsClusterReader &pScan);

    /**
     * Reads column values based on current position of cluster reader
     *
     * @param pScan cluster reader
     *
     * @param tupleData tupledata where data will be loaded
     *
     * @param colStart starting column offset where first column will be
     * loaded
     */
    void readColVals(
        SharedLcsClusterReader &pScan, TupleDataWithBuffer &tupleData,
        uint colStart);

public:
    explicit LcsRowScanBaseExecStream();
    virtual void prepare(LcsRowScanBaseExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LcsRowScanBaseExecStream.h
