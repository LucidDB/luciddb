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

#ifndef Fennel_LcsClusterNode_Included
#define Fennel_LcsClusterNode_Included

#include "fennel/segment/SegPageLock.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_NAMESPACE

// batch compression mode values
const uint LCS_COMPRESSED   = 0;
const uint LCS_FIXED        = 1;
const uint LCS_VARIABLE     = 2;

/**
 * Batch directory representing each batch within a cluster
 */
struct LcsBatchDir
{
public:
    /**
     * Type of batch:
     *  0 = compressed; only distinct values are stored for the column
     *  1 = fixed; values are stored as fixed size records
     *  2 = variable; records are stored as variable sizes but duplicates are
     *      not removed
     */
    uint16_t mode;  // 0 compressed, 1 fixed size record

    /**
     * Number of rows in the batch
     */
    uint32_t nRow;

    /**
     * Number of values in the batch.  In the case of compressed mode, this
     * is the number of distinct values.  In the other cases, it equals
     * nRows.
     */
    uint16_t nVal;

    /**
     * Number of values in the column corresponding to the batch that have
     * already been written into the cluster page
     */
    uint16_t nValHighMark;

    /**
     * Offset in the cluster page where the batch starts.
     * Contents of the batch are the following for each mode:
     *  compressed - offsets to the values followed by bit vectors
     *  variable - offsets to the values at the bottom of the page
     *  fixed - records themselves
     */
    uint16_t oVal;

    /**
     * Offset in the cluster page corresponding to the last value for this
     * batch that has already been written into the cluster page
     */
    uint16_t oLastValHighMark;  // value high mark before the batch
    
    /**
     * Size of batch records in fixed mode case
     */
    uint16_t recSize;
};

typedef LcsBatchDir *PLcsBatchDir;

/**
 * Header stored on each page of a cluster
 */
struct LcsClusterNode : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0xa83767addb0d2f09LL;
public:

    /**
     * First RID stored on cluster page
     */
    Rid firstRID;

    /**
     * Number of columns in the cluster
     */
    uint16_t nColumn;

    /**
     * Offset to batch directory
     */
    uint16_t oBatch;

    /**
     * Number of batches on cluster page
     */
    uint16_t nBatch;

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
    uint16_t *nVal;

    /**
     * For each column in the cluster, offset used to get the real offset
     * within the page
     */
    uint16_t *delta;
};

/**
 * Returns size of the header at the start of the cluster page, taking into
 * account the variable length elements that are dependent on the number of
 * columns in the cluster
 */
inline uint GetClusterSubHeaderSize(uint nColumns)
{
    return sizeof(LcsClusterNode) + (4 * sizeof(uint16_t *) * nColumns);
}

typedef LcsClusterNode *PLcsClusterNode;
typedef SegNodeLock<LcsClusterNode> ClusterPageLock;

FENNEL_END_NAMESPACE

#endif

// End LcsClusterNode.h
