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

#ifndef Fennel_NestedLoopJoinExecStream_Included
#define Fennel_NestedLoopJoinExecStream_Included

#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/DynamicParam.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Dynamic parameter used to pass a join key value from the left input to
 * the right input
 */
struct FENNEL_EXEC_EXPORT NestedLoopJoinKey
{
    DynamicParamId dynamicParamId;
    uint leftAttributeOrdinal;

    NestedLoopJoinKey(DynamicParamId id, uint offset)
        : dynamicParamId(id),
        leftAttributeOrdinal(offset)
    {
    }
};

/**
 * NestedLoopJoinExecStream defines parameters for instantiating a
 * NestedLoopJoinExecStream.
 */
struct FENNEL_EXEC_EXPORT NestedLoopJoinExecStreamParams
    : public CartesianJoinExecStreamParams
{
    std::vector<NestedLoopJoinKey> leftJoinKeys;
};

/**
 * NestedLoopJoinExecStream performs a nested loop join between two inputs
 * by iterating over the first input once and opening and re-iterating over the
 * second input for each tuple from the first.  Join keys from the first
 * (left) input are passed to the second (right) input through dynamic
 * parameters.  An optional third input will do any pre-processing required
 * prior to executing the actual nested loop join.
 *
 * <p>
 * NOTE: The input that does pre-processing needs to be the third input because
 * it may need to be opened before other streams in the overall stream graph.
 * Due to the reverse topological sort order in which a stream graph is opened,
 * the last input into a stream is opened before any of the other inputs.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT NestedLoopJoinExecStream
    : public CartesianJoinExecStream
{
    /**
     * True if pre-processing on third input completed
     */
    bool preProcessingDone;

    /**
     * Dynamic parameters corresponding to the left join keys
     */
    std::vector<NestedLoopJoinKey> leftJoinKeys;

    virtual bool checkNumInputs();

    /**
     * Creates temporary index used in nested loop join
     *
     * @return EXECRC_BUF_UNDERFLOW if request to create temporary index
     * hasn't been initiated yet
     */
    virtual ExecStreamResult preProcessRightInput();

    /**
     * Passes join keys from the left input to the right input using dynamic
     * parameters
     */
    virtual void processLeftInput();

public:
    // implement ExecStream
    virtual void prepare(NestedLoopJoinExecStreamParams const &params);
    virtual void open(bool restart);
};

FENNEL_END_NAMESPACE

#endif

// End NestedLoopJoinExecStream.h
