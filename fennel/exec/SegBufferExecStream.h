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

#ifndef Fennel_SegBufferExecStream_Included
#define Fennel_SegBufferExecStream_Included

#include "fennel/exec/ConduitExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferExecStreamParams defines parameters for instantiating a
 * SegBufferExecStream.
 *
 *<p>
 *
 * TODO:  support usage of a SpillOutputStream.
 */
struct FENNEL_EXEC_EXPORT SegBufferExecStreamParams
    : public ConduitExecStreamParams
{
    /**
     * If true, buffer contents are preserved rather than deleted as they are
     * read.  This allows open(restart=true) to be used to perform multiple
     * iterations over the buffer.
     *
     *<p>
     *
     * TODO: support "tee" on the first pass.
     */
    bool multipass;
};

/**
 * SegBufferExecStream fully buffers its input (using segment storage as
 * specified in its parameters).  The first execute request causes all input
 * data to be stored, after which the original input stream is ignored and data
 * is returned from the stored buffer instead.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SegBufferExecStream
    : public ConduitExecStream
{
    SegmentAccessor bufferSegmentAccessor;
    SharedSegBufferReader pSegBufferReader;
    SharedSegBufferWriter pSegBufferWriter;
    PageId firstPageId;
    bool multipass;

    void destroyBuffer();
    void openBufferForRead(bool destroy);

public:
    virtual void prepare(SegBufferExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
    virtual ExecStreamBufProvision getInputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferExecStream.h
