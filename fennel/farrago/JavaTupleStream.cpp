/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/farrago/JavaTupleStream.h"
#include "fennel/farrago/JniUtil.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaTupleStream::JavaTupleStream()
{
    javaByteBuffer = NULL;
}

void JavaTupleStream::prepare(JavaTupleStreamParams const &params)
{
    TupleStream::prepare(params);
    assert(!pGraph->getInputCount(getStreamId()));
    pStreamGraphHandle = params.pStreamGraphHandle;
    javaTupleStreamId = params.javaTupleStreamId;
    outputTupleDesc = params.outputTupleDesc;
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

TupleDescriptor const &JavaTupleStream::getOutputDesc() const
{
    return outputTupleDesc;
}

void JavaTupleStream::getResourceRequirements(
    ExecutionStreamResourceQuantity &minQuantity,
    ExecutionStreamResourceQuantity &optQuantity)
{
    ExecutionStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for scratch buffer
    minQuantity.nCachePages += 1;
    
    optQuantity = minQuantity;
}

void JavaTupleStream::open(bool restart)
{
    TupleStream::open(restart);

    JniEnvAutoRef pEnv;
    
    if (restart) {
        if (javaTupleStream) {
            pEnv->CallVoidMethod(
                javaTupleStream,JniUtil::methRestart);
        }
        return;
    }
    
    bufferLock.allocatePage();
    jlong hJavaTupleStream = pEnv->CallLongMethod(
        pStreamGraphHandle->javaRuntimeContext,JniUtil::methGetJavaStreamHandle,
        javaTupleStreamId);
    javaTupleStream = CmdInterpreter::getObjectFromLong(hJavaTupleStream);
    assert(javaTupleStream);
    javaByteBuffer = pEnv->NewDirectByteBuffer(
        bufferLock.getPage().getWritableData(),
        bufferLock.getPage().getCache().getPageSize());
    javaByteBuffer = pEnv->NewGlobalRef(javaByteBuffer);
    assert(javaByteBuffer);
}

void JavaTupleStream::readNextBuffer()
{
    JniEnvAutoRef pEnv;
    assert(javaTupleStream);
    uint cb = pEnv->CallIntMethod(
        javaTupleStream,JniUtil::methFillBuffer,javaByteBuffer);
    if (cb) {
        setBuffer(
            bufferLock.getPage().getWritableData(),
            cb);
    } else {
        nullifyBuffer();
    }
}

ByteInputStream &JavaTupleStream::getProducerResultStream()
{
    return *this;
}

void JavaTupleStream::closeImpl()
{
    JniEnvAutoRef pEnv;
    if (javaByteBuffer) {
        pEnv->DeleteGlobalRef(javaByteBuffer);
        javaByteBuffer = NULL;
    }
    javaTupleStream = NULL;
    bufferLock.unlock();
    TupleStream::closeImpl();
}

TupleStream::BufferProvision
JavaTupleStream::getResultBufferProvision() const
{
    return PRODUCER_PROVISION;
}

TupleStream::BufferProvision
JavaTupleStream::getInputBufferRequirement() const
{
    return NO_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End JavaTupleStream.cpp
