/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_FtrsTableWriter_Included
#define Fennel_FtrsTableWriter_Included

#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/txn/LogicalTxnParticipant.h"

FENNEL_BEGIN_NAMESPACE

class SXMutex;

struct FtrsTableIndexWriterParams : public BTreeInsertExecStreamParams
{
    TupleProjection inputProj;

    bool updateInPlace;
};

/**
 * FtrsTableWriterParams defines parameters for instantiating a FtrsTableWriter.
 */
struct FtrsTableWriterParams : public ConduitExecStreamParams 
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

struct FtrsTableIndexWriter 
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
class FtrsTableWriter : public LogicalTxnParticipant
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
