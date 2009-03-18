/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lucidera/bitmap/LbmRidReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

/**
 * The array below provides a quick method of determining the first bit
 * set in a byte value and therefore, how many bits to  shift to the right
 * to locate the first set bit.  To use the array, index into the array
 * using the current byte value.  Whatever value is returned from the array
 * is the number of bits that need to be shifted.
 */
static const uint firstOneBit[256] = {
8, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0
};

void LbmRidReaderBase::initCommon()
{
    firstReadDone = false;
    nextRid = LcsRid(0);
    resetState();
}

void LbmRidReaderBase::resetState()
{
    // set state information so that next call to readRidAndAdvance
    // will read "nextRid"
    moveNext = false;
    curByte = 0;
    curRid = LcsRid(7);
}

ExecStreamResult LbmRidReaderBase::searchForNextRid()
{
    // recompute bitOffset in current byte base on current rid
    uint bitOffset = opaqueToInt(curRid) % LbmOneByteSize;

    // get current byte (local copy)
    uint b = curByte;

    // if the moveNext flag is set, it means that we are still positioned
    // at the bit the user already read, so move forward one bit
    if (moveNext) {
        bitOffset++;
        b >>= 1;
        moveNext = false;
    }

    while (true) {
        // scan forward until we hit a bit that is set, or until we've
        // used up all the set bits

        uint shift = firstOneBit[b];
        b >>= shift;
        bitOffset += shift;

        // if we didn't find anything, then get another byte and try again
        if (b == 0) {
            bitOffset = 0;

            ExecStreamResult rc = segmentReader.advanceToRid(nextRid);
            if (rc != EXECRC_YIELD) {
                resetState();
                return rc;
            }
            firstReadDone = true;

            // get start rid of the segment just read
            uint len;
            PBuffer pB;
            segmentReader.readCurrentByteSegment(curRid, pB, len);
            b = *pB;

            nextRid = curRid + LbmOneByteSize;
        } else {
            break;
        }
    }

    // compute current rid, now that we've found a set bit
    curRid = roundToByteBoundary(curRid) + bitOffset;
    curByte = b;

    return EXECRC_YIELD;
}

ExecStreamResult LbmRidReaderBase::advanceToRid(LcsRid rid)
{
    assert(firstReadDone);

    // if we are marked for an advance, then treat this rid
    // as at least curRid + 1
    if (moveNext) {
        if (rid < curRid + 1) {
            rid = curRid + 1;
        }
        moveNext = false;
    }

    // do we need a new byte?
    if (rid >= roundToByteBoundary(curRid) + LbmOneByteSize) {
        // mark current byte as invalid, so that call to search
        // will read in a new byte with the desired rid
        curByte = 0;
        nextRid = rid;

        // read in the new byte
        ExecStreamResult rc = searchForNextRid();
        if (rc != EXECRC_YIELD) {
            return rc;
        }
    }

    // first, move forward to current byte in desired rid
    if (rid > curRid) {
        curByte >>= opaqueToInt(rid - curRid);
        curRid = rid;
    }

    // then, advance to first bit that is set
    return searchForNextRid();
}

ExecStreamResult LbmRidReaderBase::readRidAndAdvance(LcsRid &rid)
{
    // advance to position we are supposed to be at, and then keep
    // advancing until we hit a set bit
    ExecStreamResult rc = searchForNextRid();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    // set return value
    rid = curRid;

    // remember to advance the next time this method is called
    moveNext = true;

    return EXECRC_YIELD;
}

void LbmRidReader::init(
    SharedExecStreamBufAccessor &pInAccessor,
    TupleData &bitmapSegTuple)
{
    segmentReader.init(pInAccessor, bitmapSegTuple);
    initCommon();
}

void LbmIterableRidReader::initCommon()
{
    LbmRidReaderBase::initCommon();
    buffered = false;
}

void LbmTupleRidReader::init(TupleData &bitmapSegTuple)
{
    if (!pSharedReader) {
        pReader = new LbmSingleTupleReader();
        pSharedReader.reset(pReader);
    }
    pReader->init(bitmapSegTuple);

    segmentReader.init(pSharedReader, bitmapSegTuple);
    LbmIterableRidReader::initCommon();
}

LbmDeletionIndexReader::~LbmDeletionIndexReader()
{
    endSearch();
}

void LbmDeletionIndexReader::init(
    SharedBTreeReader &btreeReaderInit,
    TupleData &bitmapSegTuple)
{
    // the btree must be a deletion index
    assert(btreeReaderInit->getTupleDescriptor().size() == 3);
    assert(bitmapSegTuple.size() == 3);

    btreeReader = btreeReaderInit;
    pBitmapSegTuple = &bitmapSegTuple;
    searchEntry.compute(btreeReader->getKeyDescriptor());
    emptyIndexUnknown = true;
    currTuple = false;
}

void LbmDeletionIndexReader::initRidReader()
{
    currTuple = true;
    ridReader.init(*pBitmapSegTuple);
    btreeRid = ridReader.getNext();
}

void LbmDeletionIndexReader::endSearch()
{
    if (btreeReader) {
        btreeReader->endSearch();
    }
}

bool LbmDeletionIndexReader::isEmpty()
{
    if (emptyIndexUnknown) {
        emptyIndex = !btreeReader->searchFirst();
        emptyIndexUnknown = false;
    }
    return emptyIndex;
}

bool LbmDeletionIndexReader::searchForRid(LcsRid rid)
{
    if (isEmpty()) {
        return false;
    }

    LcsRid prevSrid = LcsRid(0);
    if (currTuple) {
        prevSrid = LbmEntry::getStartRid(*pBitmapSegTuple);
    }

    searchEntry[0].pData = reinterpret_cast<PConstBuffer>(&rid);
    btreeReader->searchForKey(searchEntry, DUP_SEEK_BEGIN, false);
    btreeReader->getTupleAccessorForRead().unmarshal(*pBitmapSegTuple);
    LcsRid foundSrid = LbmEntry::getStartRid(*pBitmapSegTuple);

    // determine whether the tuple rid reader must be restarted. it should
    // be restarted if the tuple has changed or if the rid we are searching
    // for would be positioned before the last rid read.
    bool sameTuple = (currTuple && prevSrid == foundSrid);
    if (!sameTuple || (rid < btreeRid)) {
        initRidReader();
    }

    // advance within a tuple to search for the rid
    while (btreeRid < rid) {
        if (ridReader.hasNext()) {
            btreeRid = ridReader.getNext();
        } else {
            break;
        }
    }
    return (btreeRid == rid);
}

FENNEL_END_CPPFILE("$Id: //open/dev/fennel/lucidera/bitmap/LbmRidReader.cpp#9 $");

// End LbmRidReader.cpp
