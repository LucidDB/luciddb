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
#include "fennel/farrago/JavaPullSourceExecStream.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaPullSourceExecStream::JavaPullSourceExecStream()
{
    javaByteBuffer = NULL;
}

void JavaPullSourceExecStream::prepare(JavaPullSourceExecStreamParams const &params)
{
    JavaSourceExecStream::prepare(params);
}


void JavaPullSourceExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    JavaSourceExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for scratch buffer
    minQuantity.nCachePages += 1;
    optQuantity = minQuantity;
}

void JavaPullSourceExecStream::open(bool restart)
{
    JavaSourceExecStream::open(restart);
    if (restart) 
        return;

    JniEnvAutoRef pEnv;
    bufferLock.allocatePage();
    javaByteBuffer = pEnv->NewDirectByteBuffer(
        bufferLock.getPage().getWritableData(),
        bufferLock.getPage().getCache().getPageSize());
    javaByteBuffer = pEnv->NewGlobalRef(javaByteBuffer);
    FENNEL_TRACE(TRACE_FINER, "allocated 1 java ByteBuffer " << javaByteBuffer);
    assert(javaByteBuffer);
}

ExecStreamResult JavaPullSourceExecStream::execute(ExecStreamQuantum const &)
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

void JavaPullSourceExecStream::closeImpl()
{
    JniEnvAutoRef pEnv;
    if (javaByteBuffer) {
        pEnv->DeleteGlobalRef(javaByteBuffer);
        javaByteBuffer = NULL;
    }
    JavaSourceExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End JavaPullSourceExecStream.cpp
