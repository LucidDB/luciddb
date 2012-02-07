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
#ifndef Fennel_JavaTransformExecStream_Included
#define Fennel_JavaTransformExecStream_Included

#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/tuple/TupleData.h"
#include <string>
#include <iostream>

#include <jni.h>

FENNEL_BEGIN_NAMESPACE

struct JavaTransformExecStreamParams
    : virtual public ExecStreamParams
{
    /**
     * Mimic SingleOutputExecStreamParams, but may be uninitialized.
     */
    TupleDescriptor outputTupleDesc;
    TupleFormat outputTupleFormat;

    /**
     * Class name of java peer, a FarragoTransform TODO: Register and look-up
     * the java peer like other peers; cf JavaSinkExecStream.
     */
    std::string javaClassName;

    /**
     * StreamGraphHandle, for accessing FarragoRuntimeContext.
     */
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;

    explicit JavaTransformExecStreamParams();
};


/**
 * JavaTransformExecStream represents a sequence of Java transforms
 * encapsulated within a Fennel ExecStream.
 */
class FENNEL_FARRAGO_EXPORT JavaTransformExecStream
    : virtual public ExecStream
{
    CmdInterpreter::StreamGraphHandle *pStreamGraphHandle;
    std::string javaClassName;
    jobject outputByteBuffer1;
    jobject outputByteBuffer2;
    PBuffer pBuffer1;
    PBuffer pBuffer2;


protected:
    std::vector<SharedExecStreamBufAccessor> inAccessors;
    SharedExecStreamBufAccessor pOutAccessor;

    /**
     * Request production on empty inputs. Called by execute()
     */
    void checkEmptyInputs();

    /**
     * The Java peer, an instance of a
     * net.sf.farrago.runtime.FarragoTransform.
     */
    jobject farragoTransform;

public:
    JavaTransformExecStream();
    virtual ~JavaTransformExecStream();

    // implement ExecStream
    virtual void prepare(JavaTransformExecStreamParams const &params);
    virtual void setInputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &inAccessors);
    virtual void setOutputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &outAccessors);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getInputBufProvision() const;
    virtual ExecStreamBufProvision getOutputBufProvision() const;
    virtual ExecStreamBufProvision getOutputBufConversion() const;
};

FENNEL_END_NAMESPACE

#endif

// End JavaTransformExecStream.h
