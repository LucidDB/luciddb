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

#ifndef Fennel_DoubleInputTupleStream_Included
#define Fennel_DoubleInputTupleStream_Included

#include "fennel/xo/TupleStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * DoubleInputTupleStream is an abstract base for all TupleStream
 * implementations which consume exactly two input streams.
 */
class DoubleInputTupleStream : virtual public TupleStream
{
protected:
    SharedTupleStream pFirstInputStream;
    SharedTupleStream pSecondInputStream;
    
    virtual void closeImpl();
    
public:
    virtual void prepare(TupleStreamParams const &params);
    virtual void open(bool restart);
};

FENNEL_END_NAMESPACE

#endif

// End DoubleInputTupleStream.h
