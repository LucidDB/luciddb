/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/BTreeScan.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeScan::prepare(BTreeScanParams const &params)
{
    BTreeReadTupleStream::prepare(params);
    assert(!pGraph->getInputCount(getStreamId()));
}

void BTreeScan::open(bool restart)
{
    BTreeTupleStream::open(restart);
    if (!pReader->searchFirst()) {
        pReader->endSearch();
    }
}

bool BTreeScan::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    if (!pReader->isPositioned()) {
        return false;
    }
    
    uint cbBuffer;
    PBuffer pBuffer = resultOutputStream.getWritePointer(1,&cbBuffer);
    PBuffer pBufferEnd = pBuffer + cbBuffer;
    PBuffer pNextTuple = pBuffer;
    do {
        projAccessor.unmarshal(tupleData);
        if (!outputAccessor.isBufferSufficient(
                tupleData,pBufferEnd - pNextTuple))
        {
            break;
        }
        outputAccessor.marshal(tupleData,pNextTuple);
        pNextTuple += outputAccessor.getCurrentByteCount();
        assert(pNextTuple <= pBufferEnd);
        if (!pReader->searchNext()) {
            pReader->endSearch();
        }
    } while (pReader->isPositioned());

    // TODO:  (under parameter control) unlock current leaf and relock it on
    // next fetch
    resultOutputStream.consumeWritePointer(pNextTuple - pBuffer);
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeScan.cpp
