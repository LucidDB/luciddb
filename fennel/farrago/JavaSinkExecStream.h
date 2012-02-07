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

#ifndef Fennel_JavaSinkExecStream_Included
#define Fennel_JavaSinkExecStream_Included

#include "fennel/exec/SingleInputExecStream.h"
#include "fennel/farrago/CmdInterpreter.h"

#include <jni.h>

FENNEL_BEGIN_NAMESPACE

/**
 * JavaSinkExecStreamParams defines parameters for instantiating a
 * JavaSinkExecStream.
 */
struct JavaSinkExecStreamParams : public SingleInputExecStreamParams
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    int javaFennelPipeTupleIterId;
};

/**
 * JavaSinkExecStream reads its tuples from an upstream execution object and
 * pumps them into Java.
 *
 * @author jhyde
 * @version $Id$
 */
class FENNEL_FARRAGO_EXPORT JavaSinkExecStream
    : public SingleInputExecStream
{
    ExecStreamResult lastResult;
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    int javaFennelPipeTupleIterId;

    /// our java peer, a FennelPipeTupleIter
    jobject javaFennelPipeTupleIter;

    /// its method 'write(ByteBuffer, int byteCount)'
    jmethodID methFennelPipeTupleIter_write;

    /// its method 'getByteBuffer(int size)'
    jmethodID methFennelPipeTupleIter_getByteBuffer;

    /// java method ByteBuffer.array()
    jmethodID methByteBuffer_array;

    /// sends data to the java peer
    bool sendData(PConstBuffer src, uint size);

    /// copies into a java ByteBuffer
    void stuffByteBuffer(jobject byteBuffer, PConstBuffer src, uint size);

public:
    explicit JavaSinkExecStream();

    // implement ExecStream
    virtual void prepare(JavaSinkExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End JavaSinkExecStream.h
