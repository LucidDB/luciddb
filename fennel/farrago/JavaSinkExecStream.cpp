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

FENNEL_BEGIN_CPPFILE("$Id$");

JavaSinkExecStream::JavaSinkExecStream()
{
    pStreamGraphHandle = NULL;
    javaFennelPipeIterId = 0;
    javaFennelPipeIter = NULL;
}

void JavaSinkExecStream::prepare(JavaSinkExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);
    
    pStreamGraphHandle = params.pStreamGraphHandle;
    javaFennelPipeIterId = params.javaFennelPipeIterId;
}

void JavaSinkExecStream::open(bool restart)
{
    SingleInputExecStream::open(restart);

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
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        // Need to signal end-of-stream to Java. Do this by sending a buffer of
        // length 0. There should be 0 bytes available, so the code below
        // should do this naturally.
        assert(inAccessor.getConsumptionAvailable() == 0);
        break;
    default:
        // fall through
        break;
    }


    JniEnvAutoRef pEnv;
    PConstBuffer pInBufStart = inAccessor.getConsumptionStart();
    PConstBuffer pInBufEnd = inAccessor.getConsumptionEnd();
    uint cbInAvail = pInBufEnd - pInBufStart;

    // Wrap the contents in a ByteBuffer. Since this is a local ref, it will be
    // automatically deleted when the next method call returns.
    //
    // REVIEW: Could give the ByteBuffer a longer lifecycle.
    jobject javaByteBuf = pEnv->NewDirectByteBuffer(
        (void *) pInBufStart, cbInAvail);

    // Send to the iterator, calling the method
    //   void FennelIterPipe.write(ByteBuffer, int byteCount)
    pEnv->CallVoidMethod(
        javaFennelPipeIter,
        JniUtil::methFennelPipeIterWrite,
        javaByteBuf,
        cbInAvail);

    // Have consumed all of our input.
    inAccessor.consumeData(inAccessor.getConsumptionEnd());
    return EXECRC_BUF_UNDERFLOW;
}

void JavaSinkExecStream::closeImpl()
{
    javaFennelPipeIter = NULL;
    SingleInputExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End JavaSinkExecStream.cpp
