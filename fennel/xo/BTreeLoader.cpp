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
#include "fennel/xo/BTreeLoader.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeLoader::prepare(BTreeLoaderParams const &params)
{
    BTreeTupleStream::prepare(params);
    DoubleInputTupleStream::prepare(params);
    distinctness = params.distinctness;
    pTempSegment = params.pTempSegment;
    
    // TODO:  assert BTree TupleDescriptor matches
    // pInputStream->getOutputDesc()
    
    // NOTE:  leave zeroTupleDesc empty for now; may want to change to row
    // count

    // TODO:  implement distinctness
}

TupleDescriptor const &BTreeLoader::getOutputDesc() const
{
    return zeroTupleDesc;
}

void BTreeLoader::open(bool restart)
{
    BTreeTupleStream::open(restart);
    DoubleInputTupleStream::open(restart);

    if (restart) {
        return;
    }
    
    pBuilder.reset(
        new BTreeBuilder(
            treeDescriptor,
            pTempSegment));
}

bool BTreeLoader::writeResultToConsumerBuffer(ByteOutputStream &)
{
    executeInsertion();
    return false;
}

void BTreeLoader::executeInsertion()
{
    // TODO:  fill factor
    ByteInputStream &inputCountStream =
        pFirstInputStream->getProducerResultStream();

    // REVIEW:  use a proper TupleAccessor instead?
    RecordNum nEntries;
    inputCountStream.readValue(nEntries);
    
    ByteInputStream &inputDataStream =
        pSecondInputStream->getProducerResultStream();
    pBuilder->build(inputDataStream,nEntries);
}

void BTreeLoader::closeImpl()
{
    pBuilder.reset();
    DoubleInputTupleStream::closeImpl();
    BTreeTupleStream::closeImpl();
}

TupleStream::BufferProvision
BTreeLoader::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeLoader.cpp
