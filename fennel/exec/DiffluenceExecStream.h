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

#ifndef Fennel_DiffluenceExecStream_Included
#define Fennel_DiffluenceExecStream_Included

#include "fennel/exec/SingleInputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DiffluenceExecStreamParams defines parameters for DiffluenceExecStream.
 */
struct FENNEL_EXEC_EXPORT DiffluenceExecStreamParams
    : virtual public SingleInputExecStreamParams
{
    /**
     * Output tuple descriptor.  Currently, all outputs must have the same
     * descriptor.
     */
    TupleDescriptor outputTupleDesc;

    TupleFormat outputTupleFormat;

    explicit DiffluenceExecStreamParams();
};

/**
 * DiffluenceExecStream is an abstract base for any ExecStream with
 * multiple outputs and exactly one input.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT DiffluenceExecStream
    : virtual public SingleInputExecStream
{
protected:

    /**
     * List of output buffer accessors.
     */
    std::vector<SharedExecStreamBufAccessor> outAccessors;

    /**
     * Output tuple descriptor.  Currently, all outputs must have the same
     * descriptor.
     */
    TupleDescriptor outputTupleDesc;

public:
    // implement ExecStream
    virtual void prepare(DiffluenceExecStreamParams const &params);
    virtual void setOutputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &outAccessors);
    virtual void open(bool restart);
    /**
     * Indicate to the consumer if the buffer is provided by this exec stream
     * which is the producer.
     */
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End DiffluenceExecStream.h
