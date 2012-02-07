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

#ifndef Fennel_CartesianJoinExecStream_Included
#define Fennel_CartesianJoinExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CartesianJoinExecStreamParams defines parameters for instantiating a
 * CartesianJoinExecStream.
 *
 *<p>
 *
 * TODO:  Take a join filter?
 */
struct FENNEL_EXEC_EXPORT CartesianJoinExecStreamParams
    : public ConfluenceExecStreamParams
{
    bool leftOuter;
};

/**
 * CartesianJoinExecStream produces the Cartesian product of two input
 * streams.  The first input will be iterated only once, while the second
 * input will be opened and re-iterated for each tuple from the first input.
 * Optionally, additional processing can be applied on the records read from
 * the first input before iterating over the second input.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT CartesianJoinExecStream
    : public ConfluenceExecStream
{
protected:
    bool leftOuter;
    bool rightInputEmpty;
    TupleData outputData;
    SharedExecStreamBufAccessor pLeftBufAccessor;
    SharedExecStreamBufAccessor pRightBufAccessor;
    SharedExecStream pRightInput;
    uint nLeftAttributes;

    /**
     * @return true if the number of inputs to the stream is correct
     */
    virtual bool checkNumInputs();

    /**
     * Executes any pre-processing required on the right input
     *
     * @return EXECRC_YIELD if pre-processing successful
     */
    virtual ExecStreamResult preProcessRightInput();

    /**
     * Processes the left input after it has been read from the input stream
     */
    virtual void processLeftInput();

public:
    // implement ExecStream
    virtual void prepare(CartesianJoinExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CartesianJoinExecStream.h
