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
#include "fennel/xo/BTreeInserter.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeInserter::prepare(BTreeInserterParams const &params)
{
    BTreeTupleStream::prepare(params);
    SingleInputTupleStream::prepare(params);
    distinctness = params.distinctness;
    
    // TODO:  assert BTree TupleDescriptor matches
    // pInputStream->getOutputDesc()
    
    // NOTE:  leave zeroTupleDesc empty for now; may want to change to row
    // count
}

void BTreeInserter::getResourceRequirements(
    ExecutionStreamResourceQuantity &minQuantity,
    ExecutionStreamResourceQuantity &optQuantity)
{
    ExecutionStream::getResourceRequirements(minQuantity,optQuantity);
    
    // max number of pages locked during tree update (REVIEW),
    // including BTreeWriter's private scratch page
    minQuantity.nCachePages += 5;
    
    // TODO:  use opt to govern prefetch and come up with a good formula
    optQuantity = minQuantity;
}

TupleDescriptor const &BTreeInserter::getOutputDesc() const
{
    return zeroTupleDesc;
}

void BTreeInserter::open(bool restart)
{
    BTreeTupleStream::open(restart);
    SingleInputTupleStream::open(restart);
    
    if (restart) {
        return;
    }
    
    pWriter = newWriter();
}

bool BTreeInserter::writeResultToConsumerBuffer(ByteOutputStream &)
{
    executeInsertion();
    return false;
}

void BTreeInserter::executeInsertion()
{
    ByteInputStream &inputResultStream =
        pInputStream->getProducerResultStream();
    for (;;) {
        PConstBuffer pTupleBuf = inputResultStream.getReadPointer(1);
        if (!pTupleBuf) {
            break;
        }
        uint cb = pWriter->insertTupleFromBuffer(pTupleBuf,distinctness);
        inputResultStream.consumeReadPointer(cb);
    }
}

void BTreeInserter::closeImpl()
{
    BTreeTupleStream::closeImpl();
    pWriter.reset();
    pBTreeAccessBase.reset();
    SingleInputTupleStream::closeImpl();
}

TupleStream::BufferProvision
BTreeInserter::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeInserter.cpp
