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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/colstore/LcsClusterReader.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LcsClusterReader::LcsClusterReader(BTreeDescriptor const &treeDescriptor) :
    LcsClusterAccessBase(treeDescriptor)
{
    bTreeReader = SharedBTreeReader(new BTreeReader(treeDescriptor));
}

LcsClusterNode const &LcsClusterReader::readClusterPage()
{
    bTreeReader->getTupleAccessorForRead().unmarshal(bTreeTupleData);
    clusterPageId = readClusterPageId();
    bTreeRid = readRid();
    clusterLock.lockShared(clusterPageId);
    LcsClusterNode const &node = clusterLock.getNodeForRead();
    return node;
}

bool LcsClusterReader::getFirstClusterPageForRead(
    PConstLcsClusterNode &pBlock)
{
    bool found;

    found = bTreeReader->searchFirst();
    if (!found)
        return false;

    LcsClusterNode const &node = readClusterPage();
    pBlock = &node;
    return true;
}

bool LcsClusterReader::getNextClusterPageForRead(PConstLcsClusterNode &pBlock)
{
    bool found;

    found = bTreeReader->searchNext();
    if (!found)
        return false;

    LcsClusterNode const &node = readClusterPage();
    pBlock = &node;
    return true;
}

void LcsClusterReader::init()
{
    clusterCols.reset(new LcsColumnReader[nCols]);
    for (uint i = 0; i < nCols; i++)
        clusterCols[i].init(this, i);
}

void LcsClusterReader::open()
{
    pLeaf = NULL;
    pRangeBatches = NULL;
}

bool LcsClusterReader::position(LcsRid rid)
{
    bool found;

    if (pLeaf) {
        // Scan is already in progress. Try to find the row we want in the
        // current block.

        found = positionInBlock(rid);
        if (found)
            return true;
    } else {
        if (!bTreeReader->searchFirst()) {
            bTreeReader->endSearch();
        }
    }

    // Either this is the start of the scan or we need to read a new page
    // to locate the rid we want; find the btree record corresponding to
    // the rid

    found = searchForRid(rid);
    if (!found)
        return false;

    moveToBlock(readClusterPageId());
    
    found = positionInBlock(rid);
    // page ends before "rid"; we must be off the last block
    if (!found)
        return false;

    return true;
}

bool LcsClusterReader::searchForRid(LcsRid rid)
{
    if (!bTreeReader->isPositioned())
        return false;
    bTreeTupleData[0].pData = (PConstBuffer) &rid;
    // position on greatest lower bound of key
    bTreeReader->searchForKey(bTreeTupleData, DUP_SEEK_BEGIN,
                                           false);
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

bool LcsClusterReader::positionInBlock(LcsRid rid)
{
    // Go forward through the ranges in the current block until we find the
    // right one, or until we hit the end of the block
    while (rid >= getRangeEndRid()
           && pRangeBatches + nCols < pBatches + pLHdr->nBatch) {
        rangeStartRid += pRangeBatches->nRow;

        pRangeBatches += nCols;            // go to start of next range

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
    pLeaf = (PBuffer) pLHdr;

    pBatches = (PLcsBatchDir) (pLeaf + pLHdr->oBatch);
    rangeStartRid = pLHdr->firstRID;

    // at first range in block
    pRangeBatches = pBatches;
    // at first rid in range
    nRangePos = 0;

    // set end rowid based on already available info (for performance reasons)
    rangeEndRid = rangeStartRid + pRangeBatches->nRow;
}

FENNEL_END_CPPFILE("$Id$");

// End LcsClusterReader.cpp
