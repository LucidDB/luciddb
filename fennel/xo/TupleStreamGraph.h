/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

#ifndef Fennel_TupleStreamGraph_Included
#define Fennel_TupleStreamGraph_Included

#include "fennel/xo/ExecutionStreamGraph.h"

FENNEL_BEGIN_NAMESPACE

/**
 * A TupleStreamGraph is a connected, directed graph representing dataflow
 * among TupleStreams.  Currently, only trees are supported, so each vertex
 * except the sink has exactly one target and zero or more sources.
 */
class TupleStreamGraph : virtual public ExecutionStreamGraph
{
 public:
    static SharedTupleStreamGraph newTupleStreamGraph();
};

FENNEL_END_NAMESPACE

#endif

// End TupleStreamGraph.h
