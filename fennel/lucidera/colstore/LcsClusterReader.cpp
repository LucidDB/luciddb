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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/colstore/LcsClusterReader.h"
#include "fennel/lucidera/colstore/LcsColumnReader.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/segment/SegPageEntryIterImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LcsClusterReader::LcsClusterReader(
    BTreeDescriptor const &treeDescriptor,
    CircularBuffer<LcsRidRun> *pRidRuns)
:
    LcsClusterAccessBase(treeDescriptor),
    SegPageEntryIterSource<LcsRid>(),
    ridRunIter(pRidRuns),
    prefetchQueue(4000)
{
    bTreeReader = SharedBTreeReader(new BTreeReader(treeDescriptor));
    if (pRidRuns == NULL) {
        noPrefetch = true;
    } else {
        noPrefetch = false;
        prefetchQueue.setPrefetchSource(*this);
    }
}

LcsClusterReader::~LcsClusterReader()
{
}

void LcsClusterReader::setRootPageId(PageId rootPageId)
{
    bTreeReader->setRootPageId(rootPageId);
}

LcsClusterNode const &LcsClusterReader::readClusterPage()
{
    bTreeReader->getTupleAccessorForRead().unmarshal(bTreeTupleData);
    clusterPageId = readClusterPageId();
    // REVIEW jvs 27-Dec-2005:  What is bTreeRid used for?  Should probably
    // assert that it matches node.firstRID
    bTreeRid = readRid();
    clusterLock.lockShared(clusterPageId);
    LcsClusterNode const &node = clusterLock.getNodeForRead();
    assert(bTreeRid == node.firstRID);
    return node;
}

bool LcsClusterReader::getFirstClusterPageForRead(
    PConstLcsClusterNode &pBlock)
{
    bool found;

    found = bTreeReader->searchFirst();
    if (!found) {
        return false;
    }

    LcsClusterNode const &node = readClusterPage();
    pBlock = &node;
    return true;
}

bool LcsClusterReader::getNextClusterPageForRead(PConstLcsClusterNode &pBlock)
{
    bool found;

    found = bTreeReader->searchNext();
    if (!found) {
        return false;
    }

    LcsClusterNode const &node = readClusterPage();
    pBlock = &node;
    return true;
}

void LcsClusterReader::initColumnReaders(
    uint nClusterColsInit,
    TupleProjection const &clusterProj)
{
    nClusterCols = nClusterColsInit;
    nColsToRead = clusterProj.size();
    clusterCols.reset(new LcsColumnReader[nColsToRead]);
    for (uint i = 0; i < nColsToRead; i++) {
        clusterCols[i].init(this, clusterProj[i]);
    }
}

void LcsClusterReader::open()
{
    pLeaf = NULL;
    pRangeBatches = NULL;
    ridRunIter.reset();

    // values of (0, MAXU) indicate that entry has not been filled in yet
    nextBTreeEntry.first = PageId(0);
    nextBTreeEntry.second = LcsRid(MAXU);

    nextPrefetchEntry.first = PageId(0);
    nextPrefetchEntry.second = LcsRid(MAXU);

    dumbPrefetch = false;
    nextRid = LcsRid(0);
}

void LcsClusterReader::close()
{
    bTreeReader->endSearch();
    unlockClusterPage();
}

bool LcsClusterReader::position(LcsRid rid)
{
    bool found;

    currRid = rid;
    if (pLeaf) {
        // Scan is already in progress. Try to find the row we want in the
        // current block.

        found = positionInBlock(rid);
        if (found) {
            return true;
        }
    } else {
        if (noPrefetch) {
            if (!bTreeReader->searchFirst()) {
                bTreeReader->endSearch();
            }
        } else {
            // Initiate the first set of pre-fetches.
            prefetchQueue.mapRange(segmentAccessor, NULL_PAGE_ID);
        }
    }

    if (noPrefetch) {
        found = searchForRid(rid);
        if (!found) {
            return false;
        }
        moveToBlock(readClusterPageId());
    } else {
        found = moveToBlockWithRid(rid);
        if (!found) {
            return false;
        }
    }

    found = positionInBlock(rid);
    // page ends before "rid"; we must be off the last block
    if (!found) {
        return false;
    }

    return true;
}

bool LcsClusterReader::searchForRid(LcsRid rid)
{
    bTreeTupleData[0].pData = (PConstBuffer) &rid;
    // position on greatest lower bound of key
    bTreeReader->searchForKey(
        bTreeTupleData, DUP_SEEK_BEGIN, false);
    if (bTreeReader->isSingular()) {
        return false;
    }
    bTreeReader->getTupleAccessorForRead().unmarshal(bTreeTupleData);

    LcsRid key = readRid();
    assert(key <= rid);
    return true;
}

void LcsClusterReader::moveToBlock(PageId clusterPageId)
{
    // read the desired cluster page and initialize structures to reflect
    // page read

    clusterLock.lockShared(clusterPageId);
    LcsClusterNode const &page = clusterLock.getNodeForRead();
    pLHdr = &page;
    setUpBlock();
}

bool LcsClusterReader::moveToBlockWithRid(LcsRid rid)
{
    PageId clusterPageId;

    // Read entries from the pre-fetch queue until we either find the entry
    // within the range of our desired rid, or we exhaust the contents of
    // the queue.
    for (;;) {
        std::pair<PageId, LcsRid> &prefetchEntry =
            (nextPrefetchEntry.second == LcsRid(MAXU))
            ? *prefetchQueue
            : nextPrefetchEntry;

        clusterPageId = prefetchEntry.first;
        if (clusterPageId == NULL_PAGE_ID) {
            return false;
        }

        LcsRid prefetchRid = prefetchEntry.second;
        assert(prefetchRid <= rid);

        // Make sure this is the correct entry by checking that the rid
        // is smaller than the next entry.  We can end up with non-matching
        // entries if dumb pre-fetches are being used.
        ++prefetchQueue;
        nextPrefetchEntry = *prefetchQueue;
        if (nextPrefetchEntry.first == NULL_PAGE_ID
            || rid < nextPrefetchEntry.second)
        {
            break;
        } else {
            continue;
        }
    }

    // Now that we've located the desired page, read it.
    clusterLock.lockShared(clusterPageId);
    LcsClusterNode const &page = clusterLock.getNodeForRead();
    pLHdr = &page;
    setUpBlock();
    return true;
}

bool LcsClusterReader::positionInBlock(LcsRid rid)
{
    // Go forward through the ranges in the current block until we find the
    // right one, or until we hit the end of the block
    while (rid >= getRangeEndRid()
           && pRangeBatches + nClusterCols < pBatches + pLHdr->nBatch)
    {
        rangeStartRid += pRangeBatches->nRow;

        pRangeBatches += nClusterCols;            // go to start of next range

        // set end rowid based on already available info (for performance
        // reasons)
        rangeEndRid = rangeStartRid + pRangeBatches->nRow;
    }

    // Try to position within current batch
    if (rid < getRangeEndRid()) {
        assert(rid >= rangeStartRid);
        positionInRange(opaqueToInt(rid) - opaqueToInt(rangeStartRid));
        return true;
    } else {
        return false;
    }
}

void LcsClusterReader::setUpBlock()
{
    // setup pointers to lastVal, firstVal arrays
    pLeaf = (PBuffer) pLHdr;
    setHdrOffsets(pLHdr);

    assert(pLHdr->nBatch > 0);

    pBatches = (PLcsBatchDir) (pLeaf + pLHdr->oBatch);
    rangeStartRid = pLHdr->firstRID;

    // at first range in block
    pRangeBatches = pBatches;
    // at first rid in range
    nRangePos = 0;

    // set end rowid based on already available info (for performance reasons)
    rangeEndRid = rangeStartRid + pRangeBatches->nRow;
}

bool LcsClusterReader::advance(uint nRids)
{
    uint newPos = nRangePos + nRids;

    if (newPos < pRangeBatches->nRow) {
        nRangePos = newPos;
        return true;
    } else {
        return false;
    }
}

PageId LcsClusterReader::getNextPageForPrefetch(LcsRid &rid, bool &found)
{
    found = true;
    for (;;) {
        if (dumbPrefetch) {
            rid = currRid;
        } else {
            rid = getFetchRids(ridRunIter, nextRid, false);
        }

        // If the rid run buffer is exhausted and there are still rid runs
        // to be filled in, we're pre-fetching faster than we can fill up
        // the rid run buffers.  So, switch to dumb pre-fetch in that case.
        if (rid == LcsRid(MAXU)) {
            if (ridRunIter.done()) {
                rid = LcsRid(0);
                return NULL_PAGE_ID;
            } else {
                // TODO - Use stricter criteria on when to switch to dumb
                // pre-fetch.  E.g., if there are less than a certain number
                // of pages already pre-fetched.
                dumbPrefetch = true;
                continue;
            }
        }

        if (!(nextBTreeEntry.second == LcsRid(MAXU)
            && nextBTreeEntry.first == PageId(0)))
        {
            // If we hit the end of the btree on the last iteration, then
            // there are no more pages.
            if (nextBTreeEntry.first == NULL_PAGE_ID) {
                rid = LcsRid(0);
                return NULL_PAGE_ID;
            }

            // If the next rid to be read is on the same page as the last
            // one located, bump up the rid so we move over to the next page.
            // In the case of dumb pre-fetch, just use the next page in the
            // cluster sequence.
            if (rid < nextBTreeEntry.second) {
                if (dumbPrefetch) {
                    rid = nextBTreeEntry.second;
                    PageId pageId = nextBTreeEntry.first;
                    getNextBTreeEntry();
                    return pageId;
                }
                nextRid = nextBTreeEntry.second;
                continue;
            } else {
                // TODO - optimize by avoiding search from top of btree if
                // desired entry is within a few entries of the current
                break;
           }
        } else {
            // We haven't located any pages yet.  Initiate first btree search.
            break;
        }
    }

    bool rc = searchForRid(rid);
    if (!rc) {
        return NULL_PAGE_ID;
    }
    rid = readRid();
    PageId pageId = readClusterPageId();
    getNextBTreeEntry();
    return pageId;
}

void LcsClusterReader::getNextBTreeEntry()
{
    bool rc = bTreeReader->searchNext();
    if (!rc) {
        // Indicate that there are no more pages
        nextBTreeEntry.first = NULL_PAGE_ID;
    } else {
        bTreeReader->getTupleAccessorForRead().unmarshal(bTreeTupleData);
        nextBTreeEntry.first = readClusterPageId();
        nextBTreeEntry.second = readRid();
    }
}

LcsRid LcsClusterReader::getFetchRids(
    CircularBufferIter<LcsRidRun> &ridRunIter,
    LcsRid &nextRid,
    bool remove)
{
    for (;;) {
        if (ridRunIter.end()) {
            return LcsRid(MAXU);
        }

        LcsRidRun &currRidRun = *ridRunIter;
        if (nextRid < currRidRun.startRid) {
            nextRid = currRidRun.startRid + 1;
            return currRidRun.startRid;
        } else if (
            (currRidRun.nRids == RecordNum(MAXU)
                && nextRid >= currRidRun.startRid)
            || (nextRid < currRidRun.startRid + currRidRun.nRids))
        {
            return nextRid++;
        } else {
            ++ridRunIter;
            if (remove) {
                ridRunIter.removeFront();
            }
        }
    }
}

void LcsClusterReader::catchUp(uint parentBufPos, LcsRid parentNextRid)
{
    if (parentNextRid - 1 > nextRid) {
        nextRid = parentNextRid - 1;
    }
    if (parentBufPos > ridRunIter.getCurrPos()) {
        ridRunIter.setCurrPos(parentBufPos);
    }
}

RecordNum LcsClusterReader::getNumRows()
{
    // Read the last cluster page
    if (bTreeReader->searchLast() == false) {
        bTreeReader->endSearch();
        return RecordNum(0);
    }
    bTreeReader->getTupleAccessorForRead().unmarshal(bTreeTupleData);
    LcsClusterNode const &node = readClusterPage();

    // Then count the number of rows in each batch on that page
    RecordNum nRows = RecordNum(opaqueToInt(node.firstRID));
    PLcsBatchDir pBatch = (PLcsBatchDir) ((PBuffer) &node + node.oBatch);
    for (uint i = 0; i < node.nBatch; i += nClusterCols) {
        nRows += pBatch[i].nRow;
    }

    bTreeReader->endSearch();
    return nRows;
}

FENNEL_END_CPPFILE("$Id$");

// End LcsClusterReader.cpp
