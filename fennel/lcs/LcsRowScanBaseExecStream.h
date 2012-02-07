/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_LcsRowScanBaseExecStream_Included
#define Fennel_LcsRowScanBaseExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/UnalignedAttributeAccessor.h"
#include "fennel/common/CircularBuffer.h"
#include "fennel/lcs/LcsClusterReader.h"

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
struct LcsRowScanBaseExecStreamParams : public ConfluenceExecStreamParams
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
class FENNEL_LCS_EXPORT LcsRowScanBaseExecStream
    : public ConfluenceExecStream
{
protected:
    /**
     * Projection map that maps columns read from cluster to their position
     * in the output projection
     */
    VectorOfUint projMap;

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
     * List of the non-cluster columns that need to be projected
     */
    std::vector<int> nonClusterCols;

    /**
     * True in the special case where we are only reading special columns.
     * I.e., we don't actually have to read the underlying cluster data.
     */
    bool allSpecial;

    /**
     * Circular buffer of rid runs
     */
    CircularBuffer<LcsRidRun> ridRuns;

    /**
     * Positions column readers based on new cluster reader position
     *
     * @param pScan cluster reader
     */
    void syncColumns(SharedLcsClusterReader &pScan);

    /**
     * Accessors used for loading actual column values.
     */
    std::vector<UnalignedAttributeAccessor> attrAccessors;

    /**
     * Reads column values based on current position of cluster reader
     *
     * @param pScan cluster reader
     * @param tupleData tupledata where data will be loaded
     * @param colStart starting column offset where first column will be
     * loaded
     *
     * @return false if column filters failed; true otherwise
     */
    bool readColVals(
        SharedLcsClusterReader &pScan,
        TupleDataWithBuffer &tupleData,
        uint colStart);

    /**
     * Builds outputProj from params.
     *
     * @param outputProj the projection to be built
     *
     * @param params the LcsRowScanBaseExecStreamParams
     *
     */
    virtual void buildOutputProj(
        TupleProjection &outputProj,
        LcsRowScanBaseExecStreamParams const &params);

public:
    explicit LcsRowScanBaseExecStream();
    virtual void prepare(LcsRowScanBaseExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void closeImpl();
};

/**
 * Column ordinals used to represent "special" columns, like rid
 */
enum LcsSpecialColumnId {
    LCS_RID_COLUMN_ID = 0x7FFFFF00
};

FENNEL_END_NAMESPACE

#endif

// End LcsRowScanBaseExecStream.h
