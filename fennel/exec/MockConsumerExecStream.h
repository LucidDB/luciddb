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

#ifndef Fennel_MockConsumerExecStream_Included
#define Fennel_MockConsumerExecStream_Included

#include "fennel/exec/SingleInputExecStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TuplePrinter.h"

using std::vector;
using std::string;
using std::ostream;

FENNEL_BEGIN_NAMESPACE

/**
 * MockConsumerExecStreamParams defines parameters for MockConsumerExecStream.
 */
struct FENNEL_EXEC_EXPORT MockConsumerExecStreamParams
    : public SingleInputExecStreamParams
{
    /** save data as a vector of strings */
    bool saveData;
    /** when not null, echo data to this stream */
    ostream* echoData;

    MockConsumerExecStreamParams() : saveData(true), echoData(0)
    {
    }
};

/**
 * MockConsumerExecStream consumes data from a single input. It saves the data
 * as a vector of strings, or echoes the strings to an ostream, or both.
 *
 * @author Julian Hyde
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MockConsumerExecStream
    : public SingleInputExecStream
{
protected:
    bool saveData;
    ostream* echoData;
    vector<string> rowStrings;
private:
    long rowCount;
    TupleData inputTuple;
    TuplePrinter tuplePrinter;
    bool recvEOS;

public:
    // implement ExecStream
    virtual void prepare(MockConsumerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    long getRowCount() const
    {
        return rowCount;
    }

    const vector<string>& getRowVector() {
        return const_cast<const vector<string>& >(rowStrings);
    }

    bool getRecvEOS() const
    {
        return recvEOS;
    }
};

FENNEL_END_NAMESPACE

#endif

// End MockConsumerExecStream.h
