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

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/TupleStreamGraph.h"
#include "fennel/xo/TupleStreamGraphImpl.h"
#include "fennel/xo/TupleStream.h"
#include "fennel/segment/Segment.h"

#include <boost/bind.hpp>
#include <boost/graph/topological_sort.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

TupleStreamGraphImpl::TupleStreamGraphImpl()
{
}

SharedTupleStreamGraph TupleStreamGraph::newTupleStreamGraph()
{
    return SharedTupleStreamGraph(
        new TupleStreamGraphImpl(),ClosableObjectDestructor());
}

FENNEL_END_CPPFILE("$Id$");

// End TupleStreamGraph.cpp
