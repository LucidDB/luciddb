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

#ifndef Fennel_SebInsertExecStream_Included
#define Fennel_SebInsertExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/tuple/TupleData.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

struct SebInsertExecStreamParams : public ConduitExecStreamParams
{
    /**
     * Identifier for target table.
     */
    unsigned short tableId;
};

/**
 * SebInsertExecStream reads tuples from an upstream producer and inserts them
 * into a table via the storage engine bridge.
 *
 * @author John Sichi
 * @version $Id$
 */
class SebInsertExecStream
    : public ConduitExecStream
{
    /**
     * Identifier for target table.
     */
    unsigned short tableId;

    /**
     * Resulting number of rows.
     */
    RecordNum nTuples;

    /**
     * Whether row count has been produced.
     */
    bool isDone;

    /**
     * Buffer holding result rowcount.
     */
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * Tuple data read from input stream.
     */
    TupleData inputTuple;

    /**
     * Tuple for producing result rowcount.
     */
    TupleData outputTuple;

public:
    explicit SebInsertExecStream();

    // implement ExecStream
    virtual void prepare(SebInsertExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SebInsertExecStream.h
