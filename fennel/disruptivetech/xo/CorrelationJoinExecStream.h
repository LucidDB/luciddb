/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

#ifndef Fennel_CorrelationJoinExecStream_Included
#define Fennel_CorrelationJoinExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CorrelationJoinExecStreamParams defines parameters for instantiating a
 * CorrelationJoinExecStream.
 */
struct CorrelationJoinExecStreamParams : public ConfluenceExecStreamParams
{
    uint leftAttributeOrdinal;
    uint dynamicParamId;
};

/**
 * CorrelationJoinExecStream produces a join of two input
 * streams.  The corrleation will happen based on a given column 
 *
 * @author Wael Chatila
 * @version $Id$
 */
class CorrelationJoinExecStream : public ConfluenceExecStream
{
    TupleData outputData;
    SharedExecStreamBufAccessor pLeftBufAccessor;
    SharedExecStreamBufAccessor pRightBufAccessor;
    uint nLeftAttributes;
    uint leftAttributeOrdinal;
    uint dynamicParamId;

public:
    // implement ExecStream
    virtual void prepare(CorrelationJoinExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    void open(bool restart);
    virtual void close();
};

FENNEL_END_NAMESPACE

#endif

// End CorreleationJoinExecStream.h
