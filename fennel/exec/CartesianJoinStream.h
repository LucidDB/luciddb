/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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

#ifndef Fennel_CartesianJoinStream_Included
#define Fennel_CartesianJoinStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CartesianJoinStreamParams defines parameters for instantiating a
 * CartesianJoinStream.
 *
 *<p>
 *
 * TODO:  Take a join filter?
 */
struct CartesianJoinStreamParams : public ExecStreamParams
{
};

/**
 * CartesianJoinStream produces the Cartesian product of two input
 * streams.  The first input will be iterated only once, while the second
 * input will be opened and re-iterated for each tuple from the first
 * input.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class CartesianJoinStream : public ConfluenceExecStream
{
    TupleData outputData;
    SharedExecStreamBufAccessor pLeftBufAccessor;
    SharedExecStreamBufAccessor pRightBufAccessor;
    uint nLeftAttributes;

public:
    // implement ExecStream
    virtual void prepare(CartesianJoinStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End CartesianJoinStream.h
