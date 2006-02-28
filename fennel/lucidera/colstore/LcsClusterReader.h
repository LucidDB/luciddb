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
#include "fennel/tuple/TupleDescriptor.h"
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
 * Suppose a table has 2 column store clusters: one containing columns A, B,
 * and C, and another containing columns D and E.  To read the values of
 * columns A and C, create a LcsClusterReader on the first cluster, and two
 * LcsColumnReader onto this LcsClusterReader, one for each column, A and C.
 *
 * Here's how to use a LcsClusterReader (scan) and LcsColumnReader (colScan)
 * for doing a full table scan:
 *
 * scan.init
 * for i in 0..nClusterCols colScan[i].init
 * scan.position(first rid)
 * do
 *   for i in 0..nClusterCols colScan[i].sync
 *   do
 *     for i in 0..nClusterCols
 *         colScan[i].getCurrentValue
 *   while scan.advance(1)
 * while scan.nextRange != false
 * scan.close()
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
     * Updates class status to reflect page just read and sets header
     * values to point within the current page
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
     * Number of cluster columns that will be read
     */
    uint nColsToRead;

    /**
     * Column readers for each cluster column that will be read
     */
    boost::scoped_array<LcsColumnReader> clusterCols;

    /**
     * Constructor - takes as input treeDescriptor of btree corresponding
     * to cluster
     */
    explicit LcsClusterReader(BTreeDescriptor const &treeDescriptor);

    /**
     * Initializes cluster reader with column readers
     *
     * @param nClusterColsInit total number of columns in the cluster
     *
     * @param clusterProj list of columns to be read from cluster; column
     * numbers are 0-based relative to the cluster
     */
    void initColumnReaders(
        uint nClusterColsInit, TupleProjection const &clusterProj);

    /**
     * Initializes state variables used by cluster reader.
     */
    void open();

    /**
     * Performs shutdown on cluster reader
     */
    void close();

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
    inline bool isPositioned() const;

    /**
     * Returns first rid in a range
     */
    inline LcsRid getRangeStartRid() const;

    /**
     * Returns first rid after the end of the current range
     */
    inline LcsRid getRangeEndRid() const;

    /**
     * Returns number of rids in the range
     */
    inline uint getRangeSize() const;

    /**
     * Returns offset within the current range.  E.g., 0 if at first rid in
     * range
     */
    inline uint getRangePos() const;

    /**
     * Returns rid currently positioned at
     */
    inline LcsRid getCurrentRid() const;

    /**
     * Returns number of rids yet to be read. (E.g., 1 if we're at the
     * last rid)
     */
    inline uint getRangeRowsLeft() const;

    /** 
     * Positions scan on the rid, moving to a new range if necessary
     *
     * @param rid rid to position to
     *
     * @return false if rid is beyond end of cluster
     */
    bool position(LcsRid rid);

    /**
     * Positions scan on the first rid of the next range
     */
    inline bool nextRange();

    /**
     * Moves "nRids" forward in the current range.
     *
     * @param nRids number of rids to move forward
     *
     * @return false if we are at the endof the range and therefore
     * cannot advance the desired number of rids
     */
    bool advance(uint nRids);

    /**
     * Advances nRids forward in the current batch
     *
     * @param nRids number of rids to advance
     */
    inline void advanceWithinBatch(uint nRids);
};

inline bool LcsClusterReader::isPositioned() const
{
    return pRangeBatches != NULL;
}

inline LcsRid LcsClusterReader::getRangeStartRid() const
{
    return rangeStartRid;
}

inline LcsRid LcsClusterReader::getRangeEndRid() const
{
    return rangeEndRid;
}

inline uint LcsClusterReader::getRangeSize() const
{
    return pRangeBatches->nRow;
}

inline uint LcsClusterReader::getRangePos() const
{
    return nRangePos;
}

inline LcsRid LcsClusterReader::getCurrentRid() const
{
    return getRangeStartRid() + getRangePos();
}

inline uint LcsClusterReader::getRangeRowsLeft() const
{
    return getRangeSize() - getRangePos();
}

inline bool LcsClusterReader::nextRange()
{
    return position(getRangeEndRid());
}

inline void LcsClusterReader::advanceWithinBatch(uint nRids)
{
    nRangePos = nRangePos + nRids;
}

FENNEL_END_NAMESPACE

#endif

// End LcsClusterReader.h
