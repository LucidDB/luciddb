/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2003-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
#include "fennel/disruptivetech/xo/CalcTupleStream.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CalcTupleStream::prepare(CalcTupleStreamParams const &params)
{
    SingleInputTupleStream::prepare(params);
    CalcExecutionStream::prepare(
        params,
        pInputStream->getOutputDesc(),
        params.outputTupleDesc);
}

TupleDescriptor const &CalcTupleStream::getOutputDesc() const
{
    return CalcExecutionStream::getOutputDesc();
}

bool CalcTupleStream::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    uint cbOutputBuffer;
    PBuffer pOutputBuffer = resultOutputStream.getWritePointer(
        1,&cbOutputBuffer);
    PBuffer pOutputBufferEnd = pOutputBuffer + cbOutputBuffer;

    pOutputBufferEnd = CalcExecutionStream::execute(
        pInputStream->getProducerResultStream(),
        pOutputBuffer,
        pOutputBufferEnd);

    uint cbOutput = pOutputBufferEnd - pOutputBuffer;
    if (cbOutput) {
        resultOutputStream.consumeWritePointer(cbOutput);
        return true;
    } else {
        return false;
    }
}

void CalcTupleStream::closeImpl()
{
    CalcExecutionStream::closeImpl();
    SingleInputTupleStream::closeImpl();
}

ExecutionStream::BufferProvision
CalcTupleStream::getResultBufferProvision() const
{
    return CONSUMER_PROVISION;
}

ExecutionStream::BufferProvision
CalcTupleStream::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End CalcTupleStream.cpp
