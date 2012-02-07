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

#ifndef Fennel_FtrsTableWriter_Included
#define Fennel_FtrsTableWriter_Included

#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/txn/LogicalTxnParticipant.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

class SXMutex;

struct FENNEL_FTRS_EXPORT FtrsTableIndexWriterParams
    : public BTreeInsertExecStreamParams
{
    TupleProjection inputProj;

    bool updateInPlace;
};

/**
 * FtrsTableWriterParams defines parameters for instantiating a FtrsTableWriter.
 */
struct FENNEL_FTRS_EXPORT FtrsTableWriterParams
    : public ConduitExecStreamParams
{
    /**
     * Parameters for individual indexes making up table.
     */
    std::vector<FtrsTableIndexWriterParams> indexParams;

    /**
     * PageOwnerId for clustered index.
     */
    PageOwnerId tableId;

    /**
     * Projection of old attributes with corresponding updated attributes,
     * or empty for insert/delete.
     */
    TupleProjection updateProj;
};

struct FENNEL_FTRS_EXPORT FtrsTableIndexWriter
{
    SharedBTreeWriter pWriter;
    Distinctness distinctness;
    TupleProjection inputProj;
    TupleProjection inputKeyProj;
    TupleData tupleData;
    BTreeOwnerRootMap *pRootMap;
    bool updateInPlace;
};

/**
 * FtrsTableWriter performs inserts, updates, and deletes on the indexes making
 * up a table.
 */
class FENNEL_FTRS_EXPORT FtrsTableWriter
    : public LogicalTxnParticipant
{
    friend class FtrsTableWriterFactory;

    typedef std::vector<FtrsTableIndexWriter> IndexWriterVector;

    uint nAttrs;
    TupleAccessor tupleAccessor;
    IndexWriterVector indexWriters;
    FtrsTableIndexWriter *pClusteredIndexWriter;
    TupleProjection updateProj;
    TupleData updateTupleData;
    TupleData *pTupleData;
    boost::scoped_array<FixedBuffer> logBuf;

    FtrsTableIndexWriter &createIndexWriter(
        FtrsTableIndexWriter &,
        FtrsTableIndexWriterParams const &);
    inline void insertIntoIndex(FtrsTableIndexWriter &);
    inline void deleteFromIndex(FtrsTableIndexWriter &);
    void describeIndex(FtrsTableIndexWriter &,ByteOutputStream *pLogStream);
    void executeUpdate(bool reverse);
    inline void modifyAllIndexes(LogicalActionType);
    inline void modifySomeIndexes(
        LogicalActionType,
        IndexWriterVector::iterator &,
        IndexWriterVector::iterator);
    inline void executeTuple(LogicalActionType);
    inline void copyNewValues();
    inline void copyOldValues();
    inline bool searchForIndexKey(FtrsTableIndexWriter &);

    explicit FtrsTableWriter(FtrsTableWriterParams const &params);
    PageOwnerId getTableId();

public:
    /**
     * LogicalActionType for inserting a table tuple.
     */
    static const LogicalActionType ACTION_INSERT;

    /**
     * LogicalActionType for deleting a table tuple.
     */
    static const LogicalActionType ACTION_DELETE;

    /**
     * LogicalActionType for updating a table tuple.
     */
    static const LogicalActionType ACTION_UPDATE;

    /**
     * LogicalActionType for reversing the update of a table tuple.
     */
    static const LogicalActionType ACTION_REVERSE_UPDATE;

    /**
     * Reads all tuples from a buffer and uses them as input to perform the
     * requested action on the target table.
     *
     * @param quantum governs the amount of execution to perform
     *
     * @param bufAccessor stream buffer from which to read
     *
     * @param actionType what to to with tuples (ACTION_INSERT or
     * ACTION_DELETE)
     *
     * @param actionMutex SXMutex on which to take a shared lock while
     * action is in progress
     *
     * @return number of tuples processed
     */
    RecordNum execute(
        ExecStreamQuantum const &quantum,
        ExecStreamBufAccessor &bufAccessor,
        LogicalActionType actionType,
        SXMutex &actionMutex);

    uint getIndexCount() const;
    void openIndexWriters();
    void closeIndexWriters();

    // implement LogicalTxnParticipant
    virtual LogicalTxnClassId getParticipantClassId() const;
    virtual void describeParticipant(
        ByteOutputStream &logStream);
    virtual void undoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream);
    virtual void redoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream);

};

FENNEL_END_NAMESPACE

#endif

// End FtrsTableWriter.h
