/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_TableWriterStream_Included
#define Fennel_TableWriterStream_Included

#include "fennel/xo/TableWriter.h"
#include "fennel/xo/SingleInputTupleStream.h"
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_NAMESPACE

class TableWriterFactory;
class SXMutex;

/**
 * TableWriterStreamParams defines parameters for instantiating a
 * TableWriterStream.
 */
struct TableWriterStreamParams : public TableWriterParams
{
    SharedTableWriterFactory pTableWriterFactory;
    LogicalActionType actionType;
    SXMutex *pActionMutex;
};

/**
 * TableWriterStream reads tuples from a child stream and uses them
 * to write to all of the indexes making up a table (either INSERT or
 * DEELETE depending on prepared parameters).
 */
class TableWriterStream : public SingleInputTupleStream, private ByteInputStream
{
    /**
     * TupleDescriptor for returned row count.
     */
    TupleDescriptor countTupleDesc;

    /**
     * Resulting number of rows.
     */
    RecordNum nTuples;

    /**
     * Type of write to perform (TableWriter::ACTION_INSERT
     * or TableWriter::ACTION_DELETE).
     */
    LogicalActionType actionType;

    /**
     * SXMutex on which to take a shared lock while action is in progress.
     * Normally used to block checkpoints.
     */
    SXMutex *pActionMutex;
    
protected:
    SharedTableWriter pTableWriter;

    // implement ByteInputStream
    virtual void readNextBuffer();
    
public:
    explicit TableWriterStream();
    
    // implement TupleStream
    void prepare(TableWriterStreamParams const &params);
    virtual void getResourceRequirements(
        ExecutionStreamResourceQuantity &minQuantity,
        ExecutionStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual void closeImpl();
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual ByteInputStream &getProducerResultStream();
    virtual BufferProvision getResultBufferProvision() const;
    virtual BufferProvision getInputBufferRequirement() const;
};

FENNEL_END_NAMESPACE

#endif

// End TableWriterStream.h
