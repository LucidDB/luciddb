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

#ifndef Fennel_MockProducerExecStream_Included
#define Fennel_MockProducerExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/tuple/TupleData.h"
#include <string>
#include <iostream>

FENNEL_BEGIN_NAMESPACE

/**
 * Column generator.
 *
 * Interface for any class which generates an infinite sequence of values.
 *
 * @author Julian Hyde
 */
template <class T>
class ColumnGenerator
{
public:
    virtual ~ColumnGenerator()
    {
    }

    virtual T next() = 0;
};

typedef ColumnGenerator<int64_t> Int64ColumnGenerator;

typedef boost::shared_ptr<Int64ColumnGenerator> SharedInt64ColumnGenerator;

/**
 * MockProducerExecStreamGenerator defines an interface for generating
 * a data stream.
 */
class FENNEL_EXEC_EXPORT MockProducerExecStreamGenerator
{
public:
    virtual ~MockProducerExecStreamGenerator();

    /**
     * Generates one data value.
     *
     * @param iRow 0-based row number to generate
     * @param iCol 0-based col number to generate
     */
    virtual int64_t generateValue(uint iRow, uint iCol) = 0;
};

typedef boost::shared_ptr<MockProducerExecStreamGenerator>
    SharedMockProducerExecStreamGenerator;

typedef boost::shared_ptr<MockProducerExecStreamGenerator>
    SharedMockProducerExecStreamGenerator;

/**
 * MockProducerExecStreamParams defines parameters for MockProducerExecStream.
 */
struct FENNEL_EXEC_EXPORT MockProducerExecStreamParams
    : public SingleOutputExecStreamParams
{
    /**
     * Number of rows to generate.
     */
    uint64_t nRows;

    /**
     * Generator for row values.  If non-singular, the tuple descriptor
     * for this stream must be a single int64_t.  If singular, all output
     * is constant 0.
     */
    SharedMockProducerExecStreamGenerator pGenerator;

    /**
     * When true, save a copy of each generated tuple for later perusal.
     * Allowed only when a generator is provided.
     */
    bool saveTuples;

    /**
     * When not null, print each generated tuple to this stream, for
     * tracing or for later comparison.  Allowed only when a generator
     * is provided.
     */
    std::ostream* echoTuples;

    /**
     * Generator which determines batch size. If the generator returns a
     * non-zero value, a new batch is started. If the generator is NULL, the
     * effect is the same as a generator which always returns zero: batches are
     * created as large as possible.
     */
    SharedInt64ColumnGenerator pBatchGenerator;

    MockProducerExecStreamParams()
        : nRows(0), saveTuples(false), echoTuples(0) {}
};

/**
 * MockProducerExecStream generates mock data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT MockProducerExecStream
    : public SingleOutputExecStream
{
    uint cbTuple;
    uint64_t nRowsMax;
    uint64_t nRowsProduced;
    TupleData outputData;
    SharedMockProducerExecStreamGenerator pGenerator;
    SharedInt64ColumnGenerator pBatchGenerator;
    bool saveTuples;
    std::ostream* echoTuples;
    std::vector<std::string> savedTuples;

public:
    MockProducerExecStream();

    // implement ExecStream
    virtual void prepare(MockProducerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    const std::vector<std::string>& getRowVector() {
        return const_cast<const std::vector<std::string>&>(savedTuples);
    }

    /// Returns the number of rows emitted (which does not include rows still
    /// in the output buffer).
    uint64_t getProducedRowCount();
};

FENNEL_END_NAMESPACE

#endif

// End MockProducerExecStream.h
