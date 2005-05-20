/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_TableWriter_Included
#define Fennel_TableWriter_Included

#include "fennel/xo/BTreeInserter.h"
#include "fennel/txn/LogicalTxnParticipant.h"

FENNEL_BEGIN_NAMESPACE

class SXMutex;

struct TableIndexWriterParams : public BTreeInserterParams
{
    TupleProjection inputProj;

    bool updateInPlace;
};

/**
 * TableWriterParams defines parameters for instantiating a TableWriter.
 */
struct TableWriterParams : public TupleStreamParams 
{
    /**
     * Parameters for individual indexes making up table.
     */
    std::vector<TableIndexWriterParams> indexParams;

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

struct TableIndexWriter 
{
    SharedBTreeWriter pWriter;
    Distinctness distinctness;
    TupleProjection inputProj;
    TupleProjection inputKeyProj;
    TupleData tupleData;
    BTreeRootMap *pRootMap;
    bool updateInPlace;
};

/**
 * TableWriter performs inserts, updates, and deletes on the indexes making up
 * a table.
 */
class TableWriter : public LogicalTxnParticipant
{
    friend class TableWriterFactory;

    typedef std::vector<TableIndexWriter> IndexWriterVector;

    uint nAttrs;
    TupleAccessor tupleAccessor;
    IndexWriterVector indexWriters;
    TableIndexWriter *pClusteredIndexWriter;
    TupleProjection updateProj;
    TupleData updateTupleData;
    TupleData *pTupleData;

    TableIndexWriter &createIndexWriter(
        TableIndexWriter &,
        TableIndexWriterParams const &);
    inline void insertIntoIndex(TableIndexWriter &);
    inline void deleteFromIndex(TableIndexWriter &);
    void describeIndex(TableIndexWriter &,ByteOutputStream *pLogStream);
    void executeUpdate(bool reverse);
    inline void modifyAllIndexes(LogicalActionType);
    inline void modifySomeIndexes(
        LogicalActionType,
        IndexWriterVector::iterator &,
        IndexWriterVector::iterator);
    inline void executeTuple(LogicalActionType);
    inline void copyNewValues();
    inline void copyOldValues();
    inline bool searchForIndexKey(TableIndexWriter &);
    
    explicit TableWriter(TableWriterParams const &params);
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
     * Reads all tuples from stream and uses them as input to the requested
     * action on the target table.
     *
     * @param pInputStream stream from which to read
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
        SharedExecutionStream pInputStream,
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

// End TableWriter.h
