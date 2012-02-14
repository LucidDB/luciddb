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

#ifndef Fennel_MergeExecStream_Included
#define Fennel_MergeExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * MergeExecStreamParams defines parameters for instantiating
 * a MergeExecStream.
 */
struct FENNEL_EXEC_EXPORT MergeExecStreamParams
    : public ConfluenceExecStreamParams
{
    /**
     * Whether the stream should execute in parallel mode.
     */
    bool isParallel;

    explicit MergeExecStreamParams();
};

/**
 * MergeExecStream produces the UNION ALL of any number of inputs.  All inputs
 * must have identical tuple shape; as a result, the merge can be done
 * buffer-wise rather than tuple-wise, with no copying.
 *
 *<p>
 *
 * In non-parallel mode, the implementation fully reads input i before moving
 * on to input i+1, starting with input 0.  In parallel mode, data from any
 * input is accepted as soon as it's ready.  Other possibilities for the future
 * are non-parallel round-robin (read one buffer from input 0, 1, 2, ... and
 * then back around again over and over) and sorted (merge tuples in sorted
 * order, like the top of a merge-sort; the latter would best be done in a new
 * SortedMergeExecStream since it requires tuple-wise logic).
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MergeExecStream
    : public ConfluenceExecStream
{
    /**
     * 0-based ordinal of next input from which to read.
     */
    uint iInput;

    /**
     * Number of inputs which have reached the EOS state; this stream's
     * execution is only done once all of them have.
     */
    uint nInputsEOS;

    /**
     * A bit vector indicating exactly which inputs have reached EOS.
     * The number of bits set here should always equal nInputsEOS.
     */
    std::vector<bool> inputEOS;

    /**
     * Whether this stream is executing in parallel mode.
     */
    bool isParallel;

    /**
     * End of input buffer currently being consumed.
     */
    PConstBuffer pLastConsumptionEnd;

public:
    // implement ExecStream
    virtual void prepare(MergeExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End MergeExecStream.h
