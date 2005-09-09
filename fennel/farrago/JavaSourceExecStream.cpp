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
#include "fennel/farrago/JavaSourceExecStream.h"
#include "fennel/farrago/JniUtil.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaSourceExecStream::JavaSourceExecStream()
{
}

void JavaSourceExecStream::prepare(JavaSourceExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    
    pStreamGraphHandle = params.pStreamGraphHandle;
    javaTupleStreamId = params.javaTupleStreamId;
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

void JavaSourceExecStream::open(bool restart)
{
    FENNEL_TRACE(TRACE_FINE, "open" << (restart? " (restart)" : ""));
    SingleOutputExecStream::open(restart);

    JniEnvAutoRef pEnv;

    if (restart) {
        if (javaTupleStream) {
            pEnv->CallVoidMethod(
                javaTupleStream,JniUtil::methRestart);
        }
        return;
    }

    jlong hJavaSourceExecStream = pEnv->CallLongMethod(
        pStreamGraphHandle->javaRuntimeContext,JniUtil::methGetJavaStreamHandle,
        javaTupleStreamId);
    javaTupleStream = CmdInterpreter::getObjectFromLong(hJavaSourceExecStream);
    FENNEL_TRACE(TRACE_FINER, "found javaTupleStream " << javaTupleStream << " for stream ID " << javaTupleStreamId);
    assert(javaTupleStream);
}

void JavaSourceExecStream::closeImpl()
{
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
