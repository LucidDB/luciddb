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

#ifndef Fennel_FtrsTableWriterExecStream_Included
#define Fennel_FtrsTableWriterExecStream_Included

#include "fennel/ftrs/FtrsTableWriter.h"
#include "fennel/exec/ConduitExecStream.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

class FtrsTableWriterFactory;
class SXMutex;

/**
 * FtrsTableWriterExecStreamParams defines parameters for instantiating a
 * FtrsTableWriterExecStream.
 */
struct FENNEL_FTRS_EXPORT FtrsTableWriterExecStreamParams
    : public FtrsTableWriterParams
{
    SharedFtrsTableWriterFactory pTableWriterFactory;
    LogicalActionType actionType;
    SXMutex *pActionMutex;
};

/**
 * FtrsTableWriterExecStream reads tuples from a child stream and uses them
 * to write to all of the indexes making up a table (either INSERT or
 * DEELETE depending on prepared parameters).
 */
class FENNEL_FTRS_EXPORT FtrsTableWriterExecStream
    : public ConduitExecStream
{
    /**
     * Resulting number of rows.
     */
    RecordNum nTuples;

    /**
     * Type of write to perform (FtrsTableWriter::ACTION_INSERT
     * or FtrsTableWriter::ACTION_DELETE).
     */
    LogicalActionType actionType;

    /**
     * SXMutex on which to take a shared lock while action is in progress.
     * Normally used to block checkpoints.
     */
    SXMutex *pActionMutex;

    /**
     * Object which does the real update work.
     */
    SharedFtrsTableWriter pTableWriter;

    /**
     * Id of savepoint marking start of subtransaction, or NULL_SVPT_ID
     * if no subtransaction in progress.
     */
    SavepointId svptId;

    /**
     * Whether row count has been produced.
     */
    bool isDone;

    /**
     * Buffer holding result rowcount.
     */
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * Tuple for producing result rowcount.
     */
    TupleData outputTuple;

    void createSavepoint();
    void commitSavepoint();
    void rollbackSavepoint();

public:
    explicit FtrsTableWriterExecStream();

    // implement ExecStream
    virtual void prepare(FtrsTableWriterExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End FtrsTableWriterExecStream.h
