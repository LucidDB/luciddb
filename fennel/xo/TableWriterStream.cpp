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
#include "fennel/xo/TableWriterStream.h"
#include "fennel/xo/TableWriterFactory.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TableWriterStream::TableWriterStream()
{
    pActionMutex = NULL;
}

void TableWriterStream::prepare(TableWriterStreamParams const &params)
{
    SingleInputTupleStream::prepare(params);
    pTableWriter = params.pTableWriterFactory->newTableWriter(params);
    actionType = params.actionType;
    pActionMutex = params.pActionMutex;
    assert(pActionMutex);
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor countDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_UINT_64));
    countTupleDesc.push_back(countDesc);
}

TupleDescriptor const &TableWriterStream::getOutputDesc() const
{
    return countTupleDesc;
}

void TableWriterStream::open(bool restart)
{
    SingleInputTupleStream::open(restart);
    assert(pGraph->getTxn());
    // REVIEW:  close/restart?
    pGraph->getTxn()->addParticipant(pTableWriter);
    nTuples = MAXU;
    pTableWriter->openIndexWriters();
}

void TableWriterStream::readNextBuffer()
{
    if (!isMAXU(nTuples)) {
        // already returned some result
        nullifyBuffer();
        return;
    }
    nTuples = pTableWriter->execute(pInputStream,actionType,*pActionMutex);
    // REVIEW: The format for a 1-tuple with a single uint64_t value is just
    // the value itself (assuming alignment size no greater than 64-bit), which
    // is why this works.  But it would be cleaner to set up a proper
    // TupleAccessor.
    setBuffer(
        reinterpret_cast<PConstBuffer>(&nTuples),
        sizeof(nTuples));
}

TupleStream::BufferProvision
TableWriterStream::getResultBufferProvision() const
{
    return PRODUCER_PROVISION;
}

TupleStream::BufferProvision
TableWriterStream::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

ByteInputStream &TableWriterStream::getProducerResultStream()
{
    return *this;
}

void TableWriterStream::closeImpl()
{
    SingleInputTupleStream::closeImpl();
    if (pTableWriter) {
        pTableWriter->closeIndexWriters();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End TableWriterStream.cpp
