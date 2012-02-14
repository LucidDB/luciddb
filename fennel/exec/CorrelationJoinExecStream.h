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

#ifndef Fennel_CorrelationJoinExecStream_Included
#define Fennel_CorrelationJoinExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE


/**
 * Mapping an id to an left input column
 */
struct FENNEL_EXEC_EXPORT Correlation
{
    DynamicParamId dynamicParamId;
    uint leftAttributeOrdinal;

    Correlation(DynamicParamId id, uint offset)
        : dynamicParamId(id),
        leftAttributeOrdinal(offset)
    {
        //empty
    }
};

/**
 * CorrelationJoinExecStreamParams defines parameters for instantiating a
 * CorrelationJoinExecStream.
 */
struct FENNEL_EXEC_EXPORT CorrelationJoinExecStreamParams
    : public ConfluenceExecStreamParams
{
    std::vector<Correlation> correlations;
};

/**
 * CorrelationJoinExecStream produces a join of two input
 * streams.  The correlation will happen based on one or several
 * given columns from the left-hand side.
 *
 * @author Wael Chatila
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT CorrelationJoinExecStream
    : public ConfluenceExecStream
{
    TupleData outputData;
    SharedExecStreamBufAccessor pLeftBufAccessor;
    SharedExecStreamBufAccessor pRightBufAccessor;
    uint nLeftAttributes;
    std::vector<Correlation> correlations;

    /// number of rows read from left-hand side since open(false)
    uint leftRowCount;

public:
    // implement ExecStream
    virtual void prepare(CorrelationJoinExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    void open(bool restart);
    virtual void close();
};

FENNEL_END_NAMESPACE

#endif

// End CorrelationJoinExecStream.h
