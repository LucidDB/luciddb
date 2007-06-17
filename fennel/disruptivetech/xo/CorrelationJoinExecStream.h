/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_CorrelationJoinExecStream_Included
#define Fennel_CorrelationJoinExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE


/**
 * Mapping an id to an left input column
 */
struct Correlation 
{
    DynamicParamId dynamicParamId;
    uint leftAttributeOrdinal;

    Correlation(DynamicParamId id, uint offset) : 
        dynamicParamId(id), 
        leftAttributeOrdinal(offset)
    { 
        //empty
    }
};

/**
 * CorrelationJoinExecStreamParams defines parameters for instantiating a
 * CorrelationJoinExecStream.
 */
struct CorrelationJoinExecStreamParams : public ConfluenceExecStreamParams
{
    std::vector<Correlation> correlations;
};

/**
 * CorrelationJoinExecStream produces a join of two input
 * streams.  The corrleation will happen based on one or serveral
 * given column from the left hand side
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
    std::vector<Correlation> correlations;

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
