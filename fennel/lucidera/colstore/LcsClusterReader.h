/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include "fennel/segment/SegPageEntryIter.h"
#include "fennel/segment/SegPageEntryIterSource.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Structure representing a contiguous run of rids that need to be read.
 */
struct LcsRidRun
{
    /**
     * First rid in the run
     */
    LcsRid startRid;

    /**
     * Number of rids in the run.  MAXU indicates that the run includes all
     * remaining rids following startRid.
     */
    RecordNum nRids;
};

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
class FENNEL_LCS_EXPORT LcsClusterReader
    : public LcsClusterAccessBase, public SegPageEntryIterSource<LcsRid>
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
     * If true, do not use pre-fetch
     */
    bool noPrefetch;

    /**
     * Iterator over the circular buffer containing rid runs
     */
    CircularBufferIter<LcsRidRun> ridRunIter;

    /**
     * Pre-fetch queue for pages read from this cluster
     */
    SegPageEntryIter<LcsRid> prefetchQueue;

    /**
     * The next rid that needs to be read
     */
    LcsRid nextRid;

    /**
     * The next entry in the rid-to-pageId btree map following the one that
     * contains the last page that was pre-fetched
     */
    std::pair<PageId, LcsRid> nextBTreeEntry;

    /**
     * The next entry from the pre-fetch queue
     */
    std::pair<PageId, LcsRid> nextPrefetchEntry;

    /**
     * If true, switch has been made to dumb pre-fetch
     */
    bool dumbPrefetch;

    /**
     * The current rid that needs to be read
     */
    LcsRid currRid;

    /**
     * Reads a cluster block and sets up necessary structures to navigate
     * within the page.
     *
     * @param clusterPageId pageId of the cluster page to read
     */
    void moveToBlock(PageId clusterPageId);

    /**
     * Reads a cluster block and sets up necessary structures to navigate
     * within the page corresponding to a rid.
     *
     * @param rid the rid that determines the page that needs to be read
     *
     * @return true if a cluster block was successfully located
     */
    bool moveToBlockWithRid(LcsRid rid);

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

    /**
     * Reads the next entry in the rid-to-pageId btree map.
     */
    void getNextBTreeEntry();

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
     * Constructor
     *
     * @param treeDescriptor of btree corresponding to cluster
     *
     * @param pRidRuns pointer to circular buffer of rid runs; defaults to NULL,
     * in which case, pre-fetches are disabled
     */
    explicit LcsClusterReader(
        BTreeDescriptor const &treeDescriptor,
        CircularBuffer<LcsRidRun> *pRidRuns = NULL);

    virtual ~LcsClusterReader();

    /**
     * Sets the root pageId of the underlying btree corresponding to the
     * cluster.
     *
     * @param rootPageId the root pageId that's set
     */
    void setRootPageId(PageId rootPageId);

    /**
     * Initializes cluster reader with column readers
     *
     * @param nClusterColsInit total number of columns in the cluster
     *
     * @param clusterProj list of columns to be read from cluster; column
     * numbers are 0-based relative to the cluster
     */
    void initColumnReaders(
        uint nClusterColsInit,
        TupleProjection const &clusterProj);

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

    /**
     * Determines the next page to be pre-fetched based on the rids
     * that need to be read.
     *
     * @param [out] rid returns the rid corresponding to the page to be
     * pre-fetched
     *
     * @param [out] found true if a pre-fetch page was found
     *
     * @return the page to be pre-fetched next
     */
    virtual PageId getNextPageForPrefetch(LcsRid &rid, bool &found);

    /**
     * Retrieves the current and next rids that need to be read, based on
     * the current rid run being processed.
     *
     * @param ridRunIter iterator over the circular buffer that provides the
     * rid runs
     *
     * @param [in, out] nextRid on input, indicates the minimum rid value to
     * be retrieved; on return, the next rid that will need to be read
     *
     * @param remove if true, remove the first element from the circular
     * buffer if the rid run buffer's position is incremented
     *
     * @return the current rid that needs to be read
     */
    static LcsRid getFetchRids(
        CircularBufferIter<LcsRidRun> &ridRunIter,
        LcsRid &nextRid,
        bool remove);

    /**
     * Resynchronizes the cluster reader's prefetch position to that of the
     * parent scan.
     *
     * @param parentBufPos buffer position of the parent scan
     *
     * @param parentNextRid next rid of the parent scan
     */
    void catchUp(uint parentBufPos, LcsRid parentNextRid);

    /**
     * @return the number of rows in the cluster
     */
    RecordNum getNumRows();
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
