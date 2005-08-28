/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
#include "fennel/farrago/JavaSinkExecStream.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include <iostream>

FENNEL_BEGIN_CPPFILE("$Id$");

JavaSinkExecStream::JavaSinkExecStream()
{
    lastResult = EXECRC_QUANTUM_EXPIRED; // neutral
    pStreamGraphHandle = NULL;
    javaFennelPipeIterId = 0;
    javaFennelPipeIter = NULL;
}

void JavaSinkExecStream::prepare(JavaSinkExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);
    pStreamGraphHandle = params.pStreamGraphHandle;
    javaFennelPipeIterId = params.javaFennelPipeIterId;

    JniEnvAutoRef pEnv;
    jclass classFennelPipeIter = pEnv->FindClass(
        "net/sf/farrago/runtime/FennelPipeIterator");
    assert(classFennelPipeIter);
    methFennelPipeIterator_write = pEnv->GetMethodID(
        classFennelPipeIter, "write", "(Ljava/nio/ByteBuffer;I)V");
    assert(methFennelPipeIterator_write);
    methFennelPipeIterator_getByteBuffer = pEnv->GetMethodID(
        classFennelPipeIter, "getByteBuffer", "(I)Ljava/nio/ByteBuffer;");
    assert(methFennelPipeIterator_getByteBuffer);

    jclass classByteBuffer = pEnv->FindClass("java/nio/ByteBuffer");
    assert(classByteBuffer);
    methByteBuffer_array =
        pEnv->GetMethodID(classByteBuffer, "array", "()[B");
    assert(methByteBuffer_array);
}

void JavaSinkExecStream::open(bool restart)
{
    FENNEL_TRACE(TRACE_FINE, "open");
    SingleInputExecStream::open(restart);

    // Find our FennelPipeIterator peer
    JniEnvAutoRef pEnv;
    jlong hJavaFennelPipeIter = pEnv->CallLongMethod(
        pStreamGraphHandle->javaRuntimeContext,
        JniUtil::methGetJavaStreamHandle,
        javaFennelPipeIterId);
    javaFennelPipeIter = 
        CmdInterpreter::getObjectFromLong(hJavaFennelPipeIter);
    assert(javaFennelPipeIter);
}

ExecStreamResult JavaSinkExecStream::execute(ExecStreamQuantum const &)
{
    ExecStreamBufAccessor &inAccessor = *pInAccessor;
    switch (inAccessor.getState()) {
    case EXECBUF_EMPTY:
        // Nothing to read, so don't send anything to Java. FennelPipeIter
        // would interpret a 0-length buffer as end-of-stream, which is not the
        // case.
        FENNEL_TRACE(TRACE_FINE, "no input");
        return (lastResult = EXECRC_BUF_UNDERFLOW);
    case EXECBUF_EOS:
        // Need to signal end-of-stream to Java. Do this by sending a buffer of
        // length 0. There should be 0 bytes available, so the code below
        // should do this naturally.
        FENNEL_TRACE(TRACE_FINE, "input EOS");
        assert(inAccessor.getConsumptionAvailable() == 0);
        break;
    default:
        break;
    }

    PConstBuffer pInBufStart = inAccessor.getConsumptionStart();
    PConstBuffer pInBufEnd = inAccessor.getConsumptionEnd();
    uint nbytes = pInBufEnd - pInBufStart;
    sendData(pInBufStart, nbytes);
    if (nbytes > 0) {
        inAccessor.consumeData(pInBufEnd);
        return (lastResult = EXECRC_BUF_UNDERFLOW);
    } else
        return (lastResult = EXECRC_EOS);
}

/// sends data to java peer
void JavaSinkExecStream::sendData(PConstBuffer src, uint size)
{
    // Get an output ByteBuffer. Since this is a local ref, it will be automatically
    // deleted when the next method call returns.
    // REVIEW: Could give the ByteBuffer a longer lifecycle.
    JniEnvAutoRef pEnv;
    jobject javaByteBuf = pEnv->CallObjectMethod(
        javaFennelPipeIter, methFennelPipeIterator_getByteBuffer, size);
    assert(javaByteBuf);

    // copy the data, allowing upstream XO to produce more output
    stuffByteBuffer(javaByteBuf, src, size);

    // Send to the iterator, calling the method
    //   void FennelIterPipe.write(ByteBuffer, int byteCount)
    FENNEL_TRACE(TRACE_FINE, "call FennelPipeIterator.write " << size << " bytes");
    pEnv->CallVoidMethod(javaFennelPipeIter, methFennelPipeIterator_write,
                         javaByteBuf, size);
    FENNEL_TRACE(TRACE_FINE, "FennelPipeIterator.write returned");
}

void JavaSinkExecStream::stuffByteBuffer(jobject byteBuffer, PConstBuffer src, uint size)
{
    // TODO: lookup methods in constructor.
    // TODO: ByteBuffer with a longer life, permanently pinned.
    JniEnvAutoRef pEnv;

    // pin the byte array
    jbyteArray bufBacking = 
        static_cast<jbyteArray>(
            pEnv->CallObjectMethod(byteBuffer, methByteBuffer_array));
    jboolean copied;
    jbyte* dst = pEnv->GetByteArrayElements(bufBacking, &copied);

    // copy the data
    memcpy(dst, src, size);
    // unpin
    pEnv->ReleaseByteArrayElements(bufBacking, dst, 0);
}


void JavaSinkExecStream::closeImpl()
{
    FENNEL_TRACE(TRACE_FINE, "closing");

    // If java peer is waiting for more data, send it a final EOS
    if (lastResult != EXECRC_EOS) {
        FixedBuffer dummy[1];
        sendData(dummy, 0);
    }

    javaFennelPipeIter = NULL;
    SingleInputExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End JavaSinkExecStream.cpp
