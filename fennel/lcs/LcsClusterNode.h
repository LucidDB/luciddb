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

#ifndef Fennel_LcsClusterNode_Included
#define Fennel_LcsClusterNode_Included

#include "fennel/segment/SegPageLock.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Column store rid type
 */
DEFINE_OPAQUE_INTEGER(LcsRid, uint64_t);

// batch compression mode values

enum LcsBatchMode { LCS_COMPRESSED, LCS_FIXED, LCS_VARIABLE };

/**
 * Batch directory representing each batch within a cluster
 */
struct LcsBatchDir
{
public:
    /**
     * Type of batch:
     *  compressed - only distinct values are stored for the column
     *  fixed - values are stored as fixed size records
     *  variable - records are stored as variable sizes but duplicates are
     *      not removed
     */
    LcsBatchMode mode;

    /**
     * Number of rows in the batch
     */
    uint nRow;

    /**
     * Number of values in the batch.  In the case of compressed mode, this
     * is the number of distinct values.  In the other cases, it equals
     * nRows.
     */
    uint nVal;

    /**
     * Number of values in the column corresponding to the batch that have
     * already been written into the cluster page
     */
    uint nValHighMark;

    /**
     * Offset in the cluster page where the batch starts.
     * Contents of the batch are the following for each mode:
     *  compressed - offsets to the values followed by bit vectors
     *  variable - offsets to the values at the bottom of the page
     *  fixed - records themselves
     */
    uint oVal;

    /**
     * Offset in the cluster page corresponding to the last value for this
     * batch that has already been written into the cluster page
     */
    uint oLastValHighMark;  // value high mark before the batch

    /**
     * Size of batch records in fixed mode case
     */
    uint recSize;
};

typedef LcsBatchDir *PLcsBatchDir;

/**
 * Header stored on each page of a cluster
 *
 * Note that all fields related to offsets that are stored in arrays
 * are stored as 16-bit values to minimize space usage on the cluster page
 */
struct LcsClusterNode : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xa83767addb0d2f09LL;
public:

    /**
     * First RID stored on cluster page
     */
    LcsRid firstRID;

    /**
     * Number of columns in the cluster
     */
    uint nColumn;

    /**
     * Offset to batch directory
     */
    uint oBatch;

    /**
     * Number of batches on cluster page
     */
    uint nBatch;
};

/**
 * Returns size of the header at the start of the cluster page, taking into
 * account the variable length elements that are dependent on the number of
 * columns in the cluster
 */
inline uint getClusterSubHeaderSize(uint nColumns)
{
    return sizeof(LcsClusterNode)
        + (3 * sizeof(uint16_t *) * nColumns)
        + sizeof(uint) * nColumns;
}

typedef LcsClusterNode *PLcsClusterNode;
typedef const LcsClusterNode *PConstLcsClusterNode;
typedef SegNodeLock<LcsClusterNode> ClusterPageLock;

FENNEL_END_NAMESPACE

#endif

// End LcsClusterNode.h
