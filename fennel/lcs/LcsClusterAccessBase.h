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

#ifndef Fennel_LcsClusterAccessBase_Included
#define Fennel_LcsClusterAccessBase_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/lcs/LcsClusterNode.h"
#include "fennel/btree/BTreeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LcsClusterAccessBase is a base for classes which access cluster pages.
 */
class FENNEL_LCS_EXPORT LcsClusterAccessBase
    : public boost::noncopyable
{
protected:
    /**
     * Tuple data representing the btree key corresponding to the cluster
     * page
     */
    TupleData bTreeTupleData;

    /**
     * Accessor for segment storing both btree and cluster pages.
     */
    SegmentAccessor segmentAccessor;

    /**
     * Buffer lock for the actual cluster node pages.  Shares the same
     * segment as the btree corresponding to the cluster.
     */
    ClusterPageLock clusterLock;

    /**
     * Current cluster pageid
     */
    PageId clusterPageId;

    /**
     * Current rid in btree used to access current cluster page
     */
    LcsRid bTreeRid;

    /**
     * Number of columns in cluster
     */
    uint nClusterCols;

    /**
     * Offsets to the last value stored on the page for each column in
     * cluster
     */
    uint16_t *lastVal;

    /**
     * Offsets to the first value stored on the page for each column in
     * cluster.  Points to the end of the firstVal, so subtracting lastVal
     * from firstVal will tell you the number of bytes taken up by values for
     * a column, since lastVals are appended in front of firstVal
     */
    uint16_t *firstVal;

    /**
     * Number of distinct values in the page for each column in cluster
     */
    uint *nVal;

    /**
     * For each column in the cluster, offset used to get the real offset
     * within the page
     */
    uint16_t *delta;

    /**
     * Returns RID from btree tuple
     */
    LcsRid readRid();

    /**
     * Returns cluster pageid from btree tuple
     */
    PageId readClusterPageId();

    /**
     * Sets pointers to offset arrays in cluster page header
     *
     * @param pHdr pointer to cluster node header
     */
    void setHdrOffsets(PConstLcsClusterNode pHdr);

public:
    explicit LcsClusterAccessBase(BTreeDescriptor const &treeDescriptor);

    /**
     * Returns number of columns in cluster
     */
    uint getNumClusterCols()
    {
        return nClusterCols;
    }

    /**
     * Sets number of columns in cluster
     */
    void setNumClusterCols(uint nCols)
    {
        nClusterCols = nCols;
    }

    /**
     * Unlocks cluster page
     */
    void unlockClusterPage();
};

FENNEL_END_NAMESPACE

#endif

// End LcsClusterAccessBase.h
