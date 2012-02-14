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

#ifndef Fennel_MockResourceExecStream_Included
#define Fennel_MockResourceExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

struct FENNEL_EXEC_EXPORT MockResourceExecStreamParams
    : public SingleOutputExecStreamParams
{
    ExecStreamResourceQuantity minReqt;
    ExecStreamResourceQuantity optReqt;
    ExecStreamResourceSettingType optTypeInput;
    ExecStreamResourceQuantity expected;
};

/**
 * MockResourceExecStream is an exec stream that simply allocates scratch pages.
 * The stream takes as parameters minimum and optimum resource
 * requirements.  If it is able to allocate all pages that the resource
 * governor has assigned it, then 1 is written to the output stream, else
 * 0 is written.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MockResourceExecStream
    : public SingleOutputExecStream
{
    ExecStreamResourceQuantity minReqt;
    ExecStreamResourceQuantity optReqt;
    ExecStreamResourceSettingType optTypeInput;
    ExecStreamResourceQuantity expected;

    /**
     * For allocating scratch pages
     */
    SegmentAccessor scratchAccessor;
    SegPageLock scratchLock;

    /**
     * Number of pages the resource manager indicates should be allocated
     */
    uint numToAllocate;

    /**
     * Tupledata for the output that indicates whether or not page allocation
     * was successful
     */
    TupleData outputTuple;
    TupleAccessor *outputTupleAccessor;
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * True if page allocation completed
     */
    bool isDone;

public:
    // implement ExecStream
    virtual void prepare(MockResourceExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity,
        ExecStreamResourceSettingType &optType);
    virtual void setResourceAllocation(ExecStreamResourceQuantity &quantity);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End MockResourceExecStream.h
