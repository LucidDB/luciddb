/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/SortingStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SortingStream::prepare(SortingStreamParams const &params)
{
    assert(params.rootPageId == NULL_PAGE_ID);
    assert(!params.pRootMap);
    assert(params.tupleDesc.empty());
    
    BTreeInserterParams inserterParams(params);
    pInputStream = getTupleStreamInput(0);
    inserterParams.tupleDesc = pInputStream->getOutputDesc();
    
    BTreeInserter::prepare(inserterParams);
}

// REVIEW:  do we ever want to save results on restart?
void SortingStream::open(bool restart)
{
    sorted = false;
    BTreeBuilder builder(
        treeDescriptor,
        treeDescriptor.segmentAccessor.pSegment);
    builder.createEmptyRoot();
    treeDescriptor.rootPageId = builder.getRootPageId();

    // NOTE:  do this last so that rootPageId is available
    BTreeInserter::open(restart);
}

TupleDescriptor const &SortingStream::getOutputDesc() const
{
    return treeDescriptor.tupleDescriptor;
}

bool SortingStream::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    if (!sorted) {
        executeInsertion();
        bool found = pWriter->searchFirst();
        sorted = true;
        if (!found) {
            return false;
        }
    }

    if (!pWriter->isPositioned()) {
        return false;
    }

    bool rc = false;
    TupleAccessor const &readAccessor = pWriter->getTupleAccessorForRead();
    
    do {
        uint cbBuffer;
        PBuffer pBuffer = resultOutputStream.getWritePointer(1,&cbBuffer);
        uint cbTuple = readAccessor.getCurrentByteCount();
        if (cbBuffer < cbTuple) {
            break;
        }
        memcpy(
            pBuffer,
            readAccessor.getCurrentTupleBuf(),
            cbTuple);
        resultOutputStream.consumeWritePointer(cbTuple);
        rc = true;
        if (!pWriter->searchNext()) {
            pWriter->endSearch();
        }
    } while (pWriter->isPositioned());
    return rc;
}

void SortingStream::closeImpl()
{
    BTreeInserter::closeImpl();
    BTreeBuilder builder(
        treeDescriptor,
        treeDescriptor.segmentAccessor.pSegment);
    bool rootless = true;
    builder.truncate(rootless);
    treeDescriptor.rootPageId = NULL_PAGE_ID;
}

FENNEL_END_CPPFILE("$Id$");

// End SortingStream.cpp
