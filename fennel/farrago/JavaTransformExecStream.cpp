/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2008 The Eigenbase Project
// Copyright (C) 2006-2008 Disruptive Tech
// Copyright (C) 2006-2008 LucidEra, Inc.
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
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/farrago/JavaTransformExecStream.h"
#include "fennel/farrago/JniUtil.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaTransformExecStreamParams::JavaTransformExecStreamParams()
{
    outputTupleFormat = TUPLE_FORMAT_STANDARD;
    javaClassName = "";
}

JavaTransformExecStream::JavaTransformExecStream()
{
    pStreamGraphHandle = NULL;
    outputByteBuffer = NULL;
    farragoTransform = NULL;
}

JavaTransformExecStream::~JavaTransformExecStream()
{
}

void JavaTransformExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessorsInit)
{
    inAccessors = inAccessorsInit;
}

void JavaTransformExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessors)
{
    assert(outAccessors.size() <= 1);

    if (outAccessors.size() > 0) {
        pOutAccessor = outAccessors[0];
    }
}

void JavaTransformExecStream::prepare(
    JavaTransformExecStreamParams const &params)
{
    ExecStream::prepare(params);

    if (pOutAccessor) {
        assert(pOutAccessor->getProvision() == getOutputBufProvision());
        if (pOutAccessor->getTupleDesc().empty()) {
            assert(!params.outputTupleDesc.empty());
            pOutAccessor->setTupleShape(
                params.outputTupleDesc,
                params.outputTupleFormat);
        }
    }

    for (uint i = 0; i < inAccessors.size(); ++i) {
        assert(inAccessors[i]->getProvision() == getInputBufProvision());
    }

    javaClassName = params.javaClassName;
    pStreamGraphHandle = params.pStreamGraphHandle;

    // TODO: SWZ: 3/17/06: Avoid allocating scratch space when there's
    // no output accessor.  Also need to change getResourceRequirements.
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

void JavaTransformExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for scratch buffer
    minQuantity.nCachePages += 1;
    optQuantity = minQuantity;
}

void JavaTransformExecStream::open(bool restart)
{
    FENNEL_TRACE(TRACE_FINER, "open" << (restart ? " (restart)" : ""));
    ExecStream::open(restart);

    JniEnvAutoRef pEnv;
    if (restart) {
        if (pOutAccessor) {
            pOutAccessor->clear();
        }

        // restart inputs
        for (uint i = 0; i < inAccessors.size(); ++i) {
            inAccessors[i]->clear();
            pGraph->getStreamInput(getStreamId(),i)->open(true);
        }

        assert(farragoTransform);
        pEnv->CallVoidMethod(
            farragoTransform,
            JniUtil::methFarragoTransformRestart,
            NULL);
        return;
    }

    // find java peer (a FarragoTransform)
    FENNEL_TRACE(TRACE_FINER, "finding java peer, class " << javaClassName);
    jobject o =
        pEnv->CallObjectMethod(
            pStreamGraphHandle->javaRuntimeContext,
            JniUtil::methFarragoRuntimeContextFindFarragoTransform,
            pEnv->NewStringUTF(javaClassName.c_str()));
    assert(o);
    farragoTransform = pEnv->NewGlobalRef(o);

    // Allocate output buffer.
    bufferLock.allocatePage();
    outputByteBuffer = pEnv->NewDirectByteBuffer(
        bufferLock.getPage().getWritableData(),
        bufferLock.getPage().getCache().getPageSize());
    outputByteBuffer = pEnv->NewGlobalRef(outputByteBuffer);
    FENNEL_TRACE(
        TRACE_FINER, "allocated 1 java ByteBuffer " << outputByteBuffer);
    assert(outputByteBuffer);
}


ExecStreamResult JavaTransformExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    FENNEL_TRACE(TRACE_FINEST, "execute");

    if (pOutAccessor) {
        switch (pOutAccessor->getState()) {
        case EXECBUF_NONEMPTY:
        case EXECBUF_OVERFLOW:
            FENNEL_TRACE(TRACE_FINER, "overflow");
            return EXECRC_BUF_OVERFLOW;
        case EXECBUF_EOS:
            FENNEL_TRACE(TRACE_FINER, "eos");
            return EXECRC_EOS;
        default:
            break;
        }
    }

    for (uint i = 0; i < inAccessors.size(); ++i) {
        SharedExecStreamBufAccessor inAccessor = inAccessors[i];

        // Request production on empty inputs.
        if (inAccessor->getState() == EXECBUF_EMPTY) {
            inAccessor->requestProduction();
        }
    }

    jlong jquantum = static_cast<jlong>(quantum.nTuplesMax);

    JniEnvAutoRef pEnv;
    assert(farragoTransform);
    int cb = pEnv->CallIntMethod(
        farragoTransform,
        JniUtil::methFarragoTransformExecute,
        outputByteBuffer,
        jquantum);

    if (cb > 0) {
        assert(pOutAccessor);
        pOutAccessor->provideBufferForConsumption(
            bufferLock.getPage().getWritableData(),
            bufferLock.getPage().getWritableData() + cb);

        FENNEL_TRACE(TRACE_FINER, "wrote " << cb << " bytes");
        return EXECRC_BUF_OVERFLOW;
    } else if (cb < 0) {
        FENNEL_TRACE(TRACE_FINER, "underflow");
        return EXECRC_BUF_UNDERFLOW;
    } else {
        FENNEL_TRACE(TRACE_FINER, "marking EOS");
        if (pOutAccessor) {
            pOutAccessor->markEOS();
        }
        return EXECRC_EOS;
    }
}

void JavaTransformExecStream::closeImpl()
{
    JniEnvAutoRef pEnv;
    if (farragoTransform) {
        pEnv->DeleteGlobalRef(farragoTransform);
        farragoTransform = NULL;
    }
    if (outputByteBuffer) {
        pEnv->DeleteGlobalRef(outputByteBuffer);
        outputByteBuffer = NULL;
    }
    bufferLock.unlock();
    ExecStream::closeImpl();
}

ExecStreamBufProvision JavaTransformExecStream::getInputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamBufProvision JavaTransformExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End JavaTransformExecStream.cpp
