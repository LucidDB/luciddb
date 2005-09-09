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
#include "fennel/farrago/JavaPushSourceExecStream.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaPushSourceExecStream::JavaPushSourceExecStream()
{
}

void JavaPushSourceExecStream::prepare(JavaPushSourceExecStreamParams const &params)
{
    JavaSourceExecStream::prepare(params);
    JniEnvAutoRef pEnv;

    jclass classByteBuffer = pEnv->FindClass("java/nio/ByteBuffer");
    methBufferSize =
        pEnv->GetMethodID(classByteBuffer, "limit", "()I");

    jclass classJavaPushTupleStream =
        pEnv->FindClass("net/sf/farrago/runtime/JavaPushTupleStream");
    methOpenStream = pEnv->GetMethodID(classJavaPushTupleStream, "open", "([Ljava/nio/ByteBuffer;)V");
    methCloseStream = pEnv->GetMethodID(classJavaPushTupleStream, "close", "()V");
    methGetBuffer = pEnv->GetMethodID(
        classJavaPushTupleStream, "getBuffer", "()Ljava/nio/ByteBuffer;");
    methFreeBuffer = pEnv->GetMethodID(
        classJavaPushTupleStream, "freeBuffer", "(Ljava/nio/ByteBuffer;)V");

    rdBuffer = NULL;
}

void JavaPushSourceExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    JavaSourceExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for each buffer
    minQuantity.nCachePages += NBUFFERS;
    optQuantity = minQuantity;
}

void JavaPushSourceExecStream::open(bool restart)
{
    JavaSourceExecStream::open(restart);
    if (restart) 
        return;

    // allocate 2 direct byte buffers; provide then to java peer
    JniEnvAutoRef pEnv;
    jclass classByteBuffer = pEnv->FindClass("java/nio/ByteBuffer");
    jobjectArray bba = pEnv->NewObjectArray(NBUFFERS, classByteBuffer, NULL);
    for (int i = 0; i < NBUFFERS; i++) {
        PageId pageID = bufferLock.allocatePage();
        PBuffer bufstart = bufferLock.getPage().getWritableData();
        uint cb = bufferLock.getPage().getCache().getPageSize();
        FENNEL_TRACE(TRACE_FINER, "alloc buffer at " <<
                     bufstart << " len " << cb << " on page " << pageID);

        assert(bufstart);
        assert(cb > 0);
        jobject o = pEnv->NewDirectByteBuffer(bufstart, cb);
        assert(o);
        FENNEL_TRACE(TRACE_FINER, "direct ByteBuffer " << o << " wraps buffer " << bufstart);
        pEnv->SetObjectArrayElement(bba, i, o);
    }

    FENNEL_TRACE(TRACE_FINER, "calls JavaPushTupleStream.open");
    pEnv->CallVoidMethod(javaTupleStream, methOpenStream, bba);
    FENNEL_TRACE(TRACE_FINER, "JavaPushTupleStream.open returned");
}

ExecStreamResult JavaPushSourceExecStream::execute(ExecStreamQuantum const &)
{
    // pass unless ouput buffer (rdBuffer) has been completely consumed
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

    // swap the byte buffer for fresh data from our peer
    FENNEL_TRACE(TRACE_FINER, "reader asks for a data buffer");
    jobject newBuffer = pEnv->CallObjectMethod(javaTupleStream, methGetBuffer);
    FENNEL_TRACE(TRACE_FINE, "reader gets buffer " << newBuffer);

    if (!newBuffer)                     // no new data yet
        return EXECRC_QUANTUM_EXPIRED;  // what if we return EXECRC_BUF_UNDERFLOW?

    if (rdBuffer) {
        FENNEL_TRACE(TRACE_FINER, "reader calls JavaPushTupleStream.freeBuffer(" << rdBuffer << ")");
        pEnv->CallVoidMethod(javaTupleStream, methFreeBuffer, rdBuffer);
        FENNEL_TRACE(TRACE_FINER, "JavaPushTupleStream.freeBuffer(" << rdBuffer << ") returned");
        pEnv->DeleteGlobalRef(rdBuffer);
    }
    rdBuffer = pEnv->NewGlobalRef(newBuffer);

    int cb = pEnv->CallIntMethod(newBuffer, methBufferSize);
    FENNEL_TRACE(TRACE_FINE, "buffer size " << cb);
    if (cb == 0) {                    // empty buffer means EOS
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    } else {
        PConstBuffer bufStart =
            reinterpret_cast<PConstBuffer>(pEnv->GetDirectBufferAddress(rdBuffer));
        PConstBuffer bufEnd = bufStart + cb;
        pOutAccessor->provideBufferForConsumption(bufStart, bufEnd);
        return EXECRC_BUF_OVERFLOW;
    }
}

void JavaPushSourceExecStream::closeImpl()
{
    JniEnvAutoRef pEnv;
    if (javaTupleStream)
        pEnv->CallVoidMethod(javaTupleStream, methCloseStream);
    if (rdBuffer)
        pEnv->DeleteGlobalRef(rdBuffer);
    JavaSourceExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End JavaPushSourceExecStream.cpp
