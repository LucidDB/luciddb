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

#ifndef Fennel_FtrsTableWriterExecStream_Included
#define Fennel_FtrsTableWriterExecStream_Included

#include "fennel/ftrs/FtrsTableWriter.h"
#include "fennel/exec/ConduitExecStream.h"

FENNEL_BEGIN_NAMESPACE

class FtrsTableWriterFactory;
class SXMutex;

/**
 * FtrsTableWriterExecStreamParams defines parameters for instantiating a
 * FtrsTableWriterExecStream.
 */
struct FtrsTableWriterExecStreamParams : public FtrsTableWriterParams
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
class FtrsTableWriterExecStream
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
    
    SharedFtrsTableWriter pTableWriter;

    SavepointId svptId;

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
