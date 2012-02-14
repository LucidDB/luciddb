/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/JavaSinkExecStream.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include <iostream>

FENNEL_BEGIN_CPPFILE("$Id$");

JavaSinkExecStream::JavaSinkExecStream()
{
    lastResult = EXECRC_QUANTUM_EXPIRED; // neutral
    pStreamGraphHandle = NULL;
    javaFennelPipeTupleIterId = 0;
    javaFennelPipeTupleIter = NULL;
}

void JavaSinkExecStream::prepare(JavaSinkExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);
    pStreamGraphHandle = params.pStreamGraphHandle;
    javaFennelPipeTupleIterId = params.javaFennelPipeTupleIterId;

    JniEnvAutoRef pEnv;
    jclass classFennelPipeTupleIter = pEnv->FindClass(
        "net/sf/farrago/runtime/FennelPipeTupleIter");
    assert(classFennelPipeTupleIter);
    methFennelPipeTupleIter_write = pEnv->GetMethodID(
        classFennelPipeTupleIter, "write", "(Ljava/nio/ByteBuffer;I)V");
    assert(methFennelPipeTupleIter_write);
    methFennelPipeTupleIter_getByteBuffer = pEnv->GetMethodID(
        classFennelPipeTupleIter, "getByteBuffer", "(I)Ljava/nio/ByteBuffer;");
    assert(methFennelPipeTupleIter_getByteBuffer);

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

    // Find our FennelPipeTupleIter peer
    JniEnvAutoRef pEnv;
    jlong hJavaFennelPipeTupleIter = pEnv->CallLongMethod(
        pStreamGraphHandle->javaRuntimeContext,
        JniUtil::methGetJavaStreamHandle,
        javaFennelPipeTupleIterId);
    javaFennelPipeTupleIter =
        CmdInterpreter::getObjectFromLong(hJavaFennelPipeTupleIter);
    assert(javaFennelPipeTupleIter);
}

ExecStreamResult JavaSinkExecStream::execute(ExecStreamQuantum const &)
{
    ExecStreamBufAccessor &inAccessor = *pInAccessor;
    switch (inAccessor.getState()) {
    case EXECBUF_EMPTY:
        // Nothing to read, so don't send anything to Java. FennelPipeTupleIter
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
        FENNEL_TRACE(TRACE_FINER, "input rows:");
        getGraph().getScheduler()
            ->traceStreamBufferContents(*this, inAccessor, TRACE_FINER);
        break;
    }

    PConstBuffer pInBufStart = inAccessor.getConsumptionStart();
    PConstBuffer pInBufEnd = inAccessor.getConsumptionEnd();
    uint nbytes = pInBufEnd - pInBufStart;
    bool success = sendData(pInBufStart, nbytes);
    if (success) {
        if (nbytes > 0) {
            inAccessor.consumeData(pInBufEnd);
            return (lastResult = EXECRC_BUF_UNDERFLOW);
        } else {
            return (lastResult = EXECRC_EOS);
        }
    } else {
        return EXECRC_QUANTUM_EXPIRED;
    }
}

/// sends data to java peer
bool JavaSinkExecStream::sendData(PConstBuffer src, uint size)
{
    JniEnvAutoRef pEnv;

    // Get an output ByteBuffer. Since this is a local ref, it will be
    // automatically deleted when the next method call returns.
    // REVIEW: Could give the ByteBuffer a longer lifecycle.
    jobject javaByteBuf = pEnv->CallObjectMethod(
        javaFennelPipeTupleIter, methFennelPipeTupleIter_getByteBuffer, size);
    if (!javaByteBuf) {
        // no free buffers available
        FENNEL_TRACE(
            TRACE_FINE, "FennelPipeTupleIter.getByteBuffer returned NULL. "
            << "Free buffers not available.");
        return false;
    }

    // copy the data, allowing upstream XO to produce more output
    if (size > 0) {
        stuffByteBuffer(javaByteBuf, src, size);
    }

    // Send to the iterator, calling the method
    //   void FennelIterPipe.write(ByteBuffer, int byteCount)
    FENNEL_TRACE(
        TRACE_FINE,
        "call FennelPipeTupleIter.write " << size << " bytes");
    pEnv->CallVoidMethod(
        javaFennelPipeTupleIter, methFennelPipeTupleIter_write,
        javaByteBuf, size);
    FENNEL_TRACE(TRACE_FINE, "FennelPipeTupleIter.write returned");
    return true;
}

void JavaSinkExecStream::stuffByteBuffer(
    jobject byteBuffer,
    PConstBuffer src,
    uint size)
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

    // trace the copy
    if (isTracingLevel(TRACE_FINER)) {
        // wrap the output buffer with a buf accessor
        ExecStreamBufAccessor ba;
        ba.setProvision(BUFPROV_PRODUCER);
        ba.setTupleShape(
            pInAccessor->getTupleDesc(), pInAccessor->getTupleFormat());
        ba.clear();
        PBuffer buf = (PBuffer) dst;
        ba.provideBufferForConsumption(buf, buf + size);
        FENNEL_TRACE(TRACE_FINER, "output rows:");
        getGraph().getScheduler()
            ->traceStreamBufferContents(*this, ba, TRACE_FINER);
    }

    // unpin
    pEnv->ReleaseByteArrayElements(bufBacking, dst, 0);
}


void JavaSinkExecStream::closeImpl()
{
    FENNEL_TRACE(TRACE_FINE, "closing");

    // If java peer is waiting for more data, send it a final EOS
    if (javaFennelPipeTupleIter && (lastResult != EXECRC_EOS)) {
        FixedBuffer dummy[1];
        sendData(dummy, 0);
    }

    javaFennelPipeTupleIter = NULL;
    SingleInputExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End JavaSinkExecStream.cpp
