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
    pStreamHandle = params.pStreamHandle;
    javaTupleStreamId = params.javaTupleStreamId;
    outputTupleDesc = params.tupleDesc;
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

TupleDescriptor const &JavaTupleStream::getOutputDesc() const
{
    return outputTupleDesc;
}

void JavaTupleStream::open(bool restart)
{
    // TODO: pass restart request on to Java!  Requires support up in Farrago
    // which is currently missing.  For now we use buffering to ensure that we
    // never get here.
    assert(!restart);

    TupleStream::open(restart);

    bufferLock.allocatePage();
    JniEnvAutoRef pEnv;
    jlong hJavaTupleStream = pEnv->CallLongMethod(
        pStreamHandle->javaRuntimeContext,JniUtil::methGetJavaStreamHandle,
        javaTupleStreamId);
    javaTupleStream = CmdInterpreter::getObjectFromHandle(hJavaTupleStream);
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
