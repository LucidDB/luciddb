/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_CartesianJoinExecStream_Included
#define Fennel_CartesianJoinExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CartesianJoinExecStreamParams defines parameters for instantiating a
 * CartesianJoinExecStream.
 *
 *<p>
 *
 * TODO:  Take a join filter?
 */
struct CartesianJoinExecStreamParams : public ConfluenceExecStreamParams
{
    bool leftOuter;
};

/**
 * CartesianJoinExecStream produces the Cartesian product of two input
 * streams.  The first input will be iterated only once, while the second
 * input will be opened and re-iterated for each tuple from the first
 * input.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class CartesianJoinExecStream : public ConfluenceExecStream
{
    bool leftOuter;
    bool rightInputEmpty;
    TupleData outputData;
    SharedExecStreamBufAccessor pLeftBufAccessor;
    SharedExecStreamBufAccessor pRightBufAccessor;
    uint nLeftAttributes;

public:
    // implement ExecStream
    virtual void prepare(CartesianJoinExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CartesianJoinExecStream.h
