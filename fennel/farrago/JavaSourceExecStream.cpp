/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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
#include "fennel/farrago/JavaSourceExecStream.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaSourceExecStream::JavaSourceExecStream()
{
    javaByteBuffer = NULL;
}

void JavaSourceExecStream::prepare(JavaSourceExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    
    pStreamGraphHandle = params.pStreamGraphHandle;
    javaTupleStreamId = params.javaTupleStreamId;
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

void JavaSourceExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    SingleOutputExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for scratch buffer
    minQuantity.nCachePages += 1;
    
    optQuantity = minQuantity;
}

void JavaSourceExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);

    JniEnvAutoRef pEnv;
    
    if (restart) {
        if (javaTupleStream) {
            pEnv->CallVoidMethod(
                javaTupleStream,JniUtil::methRestart);
        }
        return;
    }

    bufferLock.allocatePage();
    jlong hJavaSourceExecStream = pEnv->CallLongMethod(
        pStreamGraphHandle->javaRuntimeContext,JniUtil::methGetJavaStreamHandle,
        javaTupleStreamId);
    javaTupleStream = CmdInterpreter::getObjectFromLong(hJavaSourceExecStream);
    assert(javaTupleStream);
    javaByteBuffer = pEnv->NewDirectByteBuffer(
        bufferLock.getPage().getWritableData(),
        bufferLock.getPage().getCache().getPageSize());
    javaByteBuffer = pEnv->NewGlobalRef(javaByteBuffer);
    assert(javaByteBuffer);
}

ExecStreamResult JavaSourceExecStream::execute(ExecStreamQuantum const &)
{
    switch(pOutAccessor->getState()) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_EOS:
        return EXECRC_EOS;
    default:
        break;
    }

    JniEnvAutoRef pEnv;
    assert(javaTupleStream);
    uint cb = pEnv->CallIntMethod(
        javaTupleStream,JniUtil::methFillBuffer,javaByteBuffer);
    if (cb) {
        pOutAccessor->provideBufferForConsumption(
            bufferLock.getPage().getWritableData(),
            bufferLock.getPage().getWritableData() + cb);
        return EXECRC_BUF_OVERFLOW;
    } else {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }
}

void JavaSourceExecStream::closeImpl()
{
    JniEnvAutoRef pEnv;
    if (javaByteBuffer) {
        pEnv->DeleteGlobalRef(javaByteBuffer);
        javaByteBuffer = NULL;
    }
    javaTupleStream = NULL;
    bufferLock.unlock();
    SingleOutputExecStream::closeImpl();
}

ExecStreamBufProvision
JavaSourceExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End JavaSourceExecStream.cpp
