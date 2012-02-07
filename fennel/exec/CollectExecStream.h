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

#ifndef Fennel_CollectExecStream_Included
#define Fennel_CollectExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * CollectExecStreamParams defines parameters for instantiating a
 * CollectExecStream.
 * @author Wael Chatila
 */
struct FENNEL_EXEC_EXPORT CollectExecStreamParams
    : public ConduitExecStreamParams
{
    //empty
};

/**
 * CollectExecStream reads all tuples from a child stream and collects them
 * into a single tuple which is written to one output tuple.
 *
 * @author Wael Chatila
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT CollectExecStream
    : public ConduitExecStream
{
private:
    TupleData outputTupleData;
    TupleData inputTupleData;
    boost::scoped_array<FixedBuffer> pOutputBuffer;
    uint bytesWritten;
    bool alreadyWrittenToOutput;

public:
    virtual void prepare(CollectExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void open(bool restart);
    virtual void close();

};

FENNEL_END_NAMESPACE

#endif

// End CollectExecStream.h
