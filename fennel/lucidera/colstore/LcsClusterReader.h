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

#ifndef Fennel_LcsClusterReader_Included
#define Fennel_LcsClusterReader_Included

#include "fennel/lucidera/colstore/LcsClusterAccessBase.h"
#include "fennel/lucidera/colstore/LcsClusterNode.h"
#include "fennel/lucidera/colstore/LcsColumnReader.h"
#include "fennel/btree/BTreeReader.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Reads blocks from a single cluster.  A column reader object
 * (LcsColumnReader) is used to read the value of a particular column at the
 * current rowid of the LcsClusterReader.
 *
 * A cluster holds the values for one or more columns.  The values of these
 * columns are held in rowid ranges.  Each range contains one batch
 * (LcsBatchDir) for each column; this batch holds the values of that column
 * for each rowid in the range.  A batch may be "fixed", "compressed", or
 * "variable".
 *
 * Suppose a table has 2 row clusters: one containing columns A, B, and C, and
 * another containing columns D and E.  To read the values of columns A and C,
 * create a LcsClusterReader on the first cluster, and a LcsColumnReader onto
 * this LcsClusterReader for columns A and C.
 *
 * Here's how to use a LcsClusterReader (Scan) and LcsColumnReader (ColScan):
 *
 * Scan.Init
 * for i in 0..nCols ColScan[i].Init
 * Scan.Position(first rid)
 * do
 *   for i in 0..nCols ColScan[i].Sync
 *   do
 *     for i in 0..nCols print ColScan[i].GetCurrentValue
 *   while Scan.AdvanceWithinBatch(1)
 * while Scan.NextRange != EOF
 * Scan.Close()
 */
class LcsClusterReader : public LcsClusterAccessBase
{
    friend class LcsColumnReader;

    /**
     * Reads btree corresponding to cluster
     */
    SharedBTreeReader bTreeReader;

    /**
     * Pointer to first batch in the current range
     */
    PLcsBatchDir pRangeBatches;

    /**
     * First rid in the current range
     */
    LcsRid rangeStartRid;

    /**
     * 1 rid past the last rid in the current range
     */
    LcsRid rangeEndRid;

    /**
     * Offset from start of current range
     */
    uint nRangePos;

    /**
     * Pointer to current cluster block
     */
    PBuffer pLeaf;

    /**
     * Pointer to header of cluster block
     */
    PConstLcsClusterNode pLHdr;

    /**
     * Batch directory
     */
    PLcsBatchDir pBatches;

    /**
     * Reads a cluster block and sets up necessary structures to navigate
     * within the page
     *
     * @param clusterPageId pageid of the cluster page to be read
     */
    void moveToBlock(PageId clusterPageId);

    /**
     * Finds the range in the current block which contains "Rid".  Does not
     * search backwards.  
     *
     * @param rid rid to be located
     *
     * @return false if "Rid" is beyond the end of the block
     */
    bool positionInBlock(LcsRid rid);

    /**
     * Positions on "pos" in the current range
     *
     * @param pos desired position
     */
    void positionInRange(uint pos)
    {
        nRangePos = pos;
    }

    /**
     * Updates class status to reflect page just read
     */
    void setUpBlock();

    /**
     * Reads a cluster page based on current btree position
     *
     * @return cluster page read
     */
    LcsClusterNode const &readClusterPage();

    /**
     * Reads btree and locates rid in tree
     *
     * @return false if rid is not within the range of the tree;
     * e.g., if it's empty
     */
    bool searchForRid(LcsRid rid);

public:
    /**
     * Number of columns in the cluster
     */
    uint nCols;

    /**
     * Column readers for each cluster column
     */
    boost::scoped_array<LcsColumnReader> clusterCols;

    /**
     * Constructor - takes as input treeDescriptor of btree corresponding
     * to cluster
     */
    explicit LcsClusterReader(BTreeDescriptor const &treeDescriptor);

    /**
     * Initializes cluster reader with column readers
     */
    void init();

    /**
     * Initializes state variables used by cluster reader.
     */
    void open();

    /**
     * Gets first page in a cluster
     *
     * @param pBlock output param returning cluster page
     * 
     * @return true if page available
     */
    bool getFirstClusterPageForRead(PConstLcsClusterNode &pBlock);

    /**
     * Gets next page in a cluster, based on current position in btree
     *
     * @param pBlock output param returning cluster page
     * 
     * @return true if page available
     */
    bool getNextClusterPageForRead(PConstLcsClusterNode &pBlock);

    /**
     * Returns true if positioned within some range in a batch
     */
    bool isPositioned() const
    {
        return pRangeBatches != NULL;
    }

    /**
     * Returns first rid in a range
     */
    LcsRid getRangeStartRid() const
    {
        return rangeStartRid;
    }

    /**
     * Returns first rid after the end of the current range
     */
    LcsRid getRangeEndRid() const
    {
        return rangeEndRid;
    }

    /**
     * Returns number of rids in the range
     */
    uint getRangeSize() const
    {
        return pRangeBatches->nRow;
    }

    /**
     * Returns offset within the current range.  E.g., 0 if at first rid in
     * range
     */
    uint getRangePos() const
    {
        return nRangePos;
    }

    /**
     * Returns rid currently positioned at
     */
    LcsRid getCurrentRid() const
    {
        return getRangeStartRid() + getRangePos();
    }

    /**
     * Returns number of rids yet to be read. (E.g., 1 if we're at the
     * last rid)
     */
    uint getRangeRowsLeft() const
    {
        return getRangeSize() - getRangePos();
    }

    /** 
     * Positions scan on the rid, moving to a new range if necessary
     *
     * @param rid rid to position to
     *
     * @return false if rid is beyond end of cluster
     */
    bool position(LcsRid rid);

    /**
     * Advances nRids forward in the current batch
     *
     * @param nRids number of rids to advance
     */
    void advanceWithinBatch(uint nRids)
    {
        nRangePos = nRangePos + nRids;
    }
};

FENNEL_END_NAMESPACE

#endif

// End LcsClusterReader.h
