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

#ifndef Fennel_CalcExecStream_Included
#define Fennel_CalcExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/calculator/CalcCommon.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CalcExecStreamParams defines parameters for instantiating a
 * CalcExecStream.
 */
struct CalcExecStreamParams : public ConduitExecStreamParams
{
    std::string program;
    bool isFilter;
    bool stopOnCalcError;
    CalcExecStreamParams()
        : program(), isFilter(false), stopOnCalcError(true) {}
};

/**
 * CalcExecStream reads tuples from a child stream and performs
 * calculations of SQL expressions.  For every input tuple which passes a
 * boolean filter expression, an output tuple is computed based on projection
 * expressions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_CALCULATOR_EXPORT CalcExecStream
    : public ConduitExecStream
{
    /**
     * TupleDescriptor for input tuples.
     */
    TupleDescriptor inputDesc;

    /**
     * TupleData for input tuples.
     */
    TupleData inputData;

    /**
     * TupleData for output tuples.
     */
    TupleData outputData;

    /**
     * The Calculator object which does the real work.
     */
    SharedCalculator pCalc;


    /**
     * If this stream filters tuples, pFilterDatum refers to the boolean
     * TupleDatum containing the filter status; otherwise, pFilterDatum is
     * NULL, and the result cardinality is always equal to the input
     * cardinality.
     */
    TupleDatum const *pFilterDatum;

    /** when true, execute() aborts on a Calculator error;
     * when false, it skips the offending row.
     */
    bool stopOnCalcError;

protected:
    /**
     * To give subclasses access to the last row written.
     */
    PConstBuffer lastInBuffer;

public:
    virtual void prepare(CalcExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CalcExecStream.h
