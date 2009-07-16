/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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

#ifndef Fennel_SebQueryExecStream_Included
#define Fennel_SebQueryExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

struct SebQueryExecStreamParams : public SingleOutputExecStreamParams
{
    unsigned short indexId;
    unsigned short tableId;
    TupleProjection outputProj;
};

/**
 * SebQueryExecStream produces tuples fetched from the storage engine bridge.
 *
 * @author John Sichi
 * @version $Id$
 */
class SebQueryExecStream : public SingleOutputExecStream
{
    unsigned short indexId;
    unsigned short tableId;
    TupleProjection outputProj;
    TupleData tupleData;
    unsigned short queryMgrId;
    bool tuplePending, done;

public:
    explicit SebQueryExecStream();

    // implement ExecStream
    virtual void prepare(SebQueryExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End SebQueryExecStream.h
