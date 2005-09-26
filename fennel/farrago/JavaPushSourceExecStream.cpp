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
    nbuffers = bufferSize = 0;
    rdBuffer = NULL;
    rdBufferStart = rdBufferEnd = rdBufferPosn = 0;
    rdBufferEOS = false;
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

    nbuffers = params.nbuffers;
    bufferSize = params.bufferSize;
    assert(nbuffers >= 0);
    assert(bufferSize >= 0);
    if (nbuffers < 2)
        nbuffers = 2;
    if (bufferSize == 0)                // default is row size
        bufferSize = pOutAccessor->getScratchTupleAccessor().getMaxByteCount();

}

void JavaPushSourceExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    JavaSourceExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for each buffer ??
    minQuantity.nCachePages += nbuffers;
    optQuantity = minQuantity;
}

void JavaPushSourceExecStream::open(bool restart)
{
    JavaSourceExecStream::open(restart);

    releaseReadBuffer();
    if (restart) 
        return;

    // allocate direct byte buffers; provide then to java peer
    // REVIEW mb 9/18/05 Why use a whole cache page for each? How to allocate better?
    JniEnvAutoRef pEnv;
    jclass classByteBuffer = pEnv->FindClass("java/nio/ByteBuffer");
    jobjectArray bba = pEnv->NewObjectArray(nbuffers, classByteBuffer, NULL);
    for (int i = 0; i < nbuffers; i++) {
        PageId pageID = bufferLock.allocatePage();
        PBuffer bufstart = bufferLock.getPage().getWritableData();
        uint cb = bufferLock.getPage().getCache().getPageSize();
        FENNEL_TRACE(TRACE_FINER, "alloc buffer at " << bufstart <<
                     " len " << bufferSize << " on page " << pageID << ", pagesize " << cb);
        assert(bufstart);
        jobject o = pEnv->NewDirectByteBuffer(bufstart, bufferSize);
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
    while(true) {                       // should respect the quantum

        if (!pOutAccessor->isProductionPossible()) {
            FENNEL_TRACE(TRACE_FINE, "output buffer full");
            return EXECRC_BUF_OVERFLOW;
        }
    
        if (rdBufferPosn >= rdBufferEnd) {
            // need more input data
            releaseReadBuffer();
            if (!getReadBuffer()) {
                FENNEL_TRACE(TRACE_FINE, "no input available");
                // sets all rdBuffer pointers
                return EXECRC_QUANTUM_EXPIRED; 
                // (Not underflow, since input is not from a regular fennel buffer,
                // and scheduler will not know when more input arrives.)
            }
            if (rdBufferEOS) {
                FENNEL_TRACE(TRACE_FINE, "input EOS");
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
        }

        // copy the buffer (or as many rows as will fit)
        PBuffer dest = pOutAccessor->getProductionStart();
        uint ncopied = 
            copyRows(pOutAccessor->getScratchTupleAccessor(),
                     dest, pOutAccessor->getProductionEnd(),
                     rdBufferPosn, rdBufferEnd);
        FENNEL_TRACE(TRACE_FINE, "copied " << ncopied << " bytes");
        if (ncopied == 0) {
            pOutAccessor->requestConsumption();
            return EXECRC_BUF_OVERFLOW;
        }
        rdBufferPosn += ncopied;
        pOutAccessor->produceData(dest + ncopied);
    }
}


void JavaPushSourceExecStream::releaseReadBuffer()
{
    if (rdBuffer == NULL)
        return;

    JniEnvAutoRef pEnv;
    assert(javaTupleStream);

    FENNEL_TRACE(TRACE_FINER, "reader calls JavaPushTupleStream.freeBuffer(" << rdBuffer << ")");
    pEnv->CallVoidMethod(javaTupleStream, methFreeBuffer, rdBuffer);
    FENNEL_TRACE(TRACE_FINER, "JavaPushTupleStream.freeBuffer(" << rdBuffer << ") returned");
    pEnv->DeleteGlobalRef(rdBuffer);

    rdBuffer = 0;
    rdBufferEOS = false;
    rdBufferStart = rdBufferEnd = rdBufferPosn = 0;
}
 
bool JavaPushSourceExecStream::getReadBuffer()
{
    JniEnvAutoRef pEnv;
    assert(!rdBuffer);
    assert(javaTupleStream);
    
    FENNEL_TRACE(TRACE_FINER, "reader asks for a data buffer");
    jobject newBuffer = pEnv->CallObjectMethod(javaTupleStream, methGetBuffer);
    FENNEL_TRACE(TRACE_FINE, "reader gets buffer " << newBuffer);
    if (!newBuffer)
        return false;

    rdBuffer = pEnv->NewGlobalRef(newBuffer);
    int cb = pEnv->CallIntMethod(newBuffer, methBufferSize);
    FENNEL_TRACE(TRACE_FINE, "buffer size " << cb);

    rdBufferEOS = (cb == 0);            // buffer size 0 indicates EOS
    rdBufferStart = reinterpret_cast<PConstBuffer>(pEnv->GetDirectBufferAddress(rdBuffer));
    rdBufferEnd = rdBufferStart + cb;
    rdBufferPosn = rdBufferStart;
    return true;
}

// TODO: factor out. Similar code appears in CopyExecStream, SimpleNexusExecStream, JavaPushSourceExecStream.
uint JavaPushSourceExecStream::copyRows(
    TupleAccessor& acc,
    PBuffer dest, PBuffer destEnd,
    PConstBuffer src, PConstBuffer srcEnd)
{
    uint navail = destEnd - dest;
    uint n  = srcEnd  - src;
    assert((navail >= 0) && (n >= 0));

    if (n > navail) {
        // what fits? span whole tuples in src buffer
        PConstBuffer p, q;
        PConstBuffer limit = src + navail;
        assert(limit <= srcEnd);
        int ct;
        for (ct = 0, p = q = src; p < limit; ct++) {
            acc.setCurrentTupleBuf(p);
            q = p;
            p += acc.getCurrentByteCount(); // forward 1 tuple
        }
        // here when p is too far, and the tuple [q, p] did not fit.
        uint nfits = q - src;
        n = nfits;
    }

    memcpy(dest, src, n);
    return n;
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

ExecStreamBufProvision
JavaPushSourceExecStream::getOutputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End JavaPushSourceExecStream.cpp
