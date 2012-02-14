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

#ifndef Fennel_SortedAggExecStream_Included
#define Fennel_SortedAggExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/AggInvocation.h"
#include "fennel/exec/AggComputer.h"
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SortedAggExecStreamParams defines parameters for SortedAggExecStream.
 */
struct FENNEL_EXEC_EXPORT SortedAggExecStreamParams
    : public ConduitExecStreamParams
{
    AggInvocationList aggInvocations;
    int groupByKeyCount;
    explicit SortedAggExecStreamParams()
    {
        groupByKeyCount = 0;
    }
};

/**
 * SortedAggExecStream aggregates its input, producing tuples of aggregate
 * function computations as output. It takes input sorted on a group key and
 * produce one output tuple per group.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SortedAggExecStream
    : public ConduitExecStream
{
    enum State {
        STATE_ACCUMULATING,
        STATE_PRODUCING,
        STATE_DONE
    };

    State state;

    AggComputerList aggComputers;
    int groupByKeyCount;

    TupleData inputTuple;
    TupleDataWithBuffer prevTuple;
    TupleData outputTuple;
    bool prevTupleValid;

    inline void clearAccumulator();
    inline void updateAccumulator();
    inline void computeOutput();

    // Methods to store and compare group by keys
    inline void copyPrevGroupByKey();
    inline void setCurGroupByKey();
    inline int  compareGroupByKeys();
    inline ExecStreamResult produce();

protected:
    virtual AggComputer *newAggComputer(
        AggFunction aggFunction,
        TupleAttributeDescriptor const *pAttrDesc);

public:
    // implement ExecStream
    virtual void prepare(SortedAggExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End SortedAggExecStream.h
