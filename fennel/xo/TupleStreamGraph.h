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

#ifndef Fennel_TupleStreamGraph_Included
#define Fennel_TupleStreamGraph_Included

#include "fennel/common/ClosableObject.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Identifier for a TupleStream relative to an instance of TupleStreamGraph.
 */
typedef uint TupleStreamId;

/**
 * A TupleStreamGraph is a connected, directed graph representing dataflow
 * among TupleStreams.  Currently, only trees are supported, so each vertex
 * except the sink has exactly one target and zero or more sources.
 */
class TupleStreamGraph : public boost::noncopyable, public ClosableObject
{
public:
    virtual ~TupleStreamGraph();

    virtual void setTxn(
        SharedLogicalTxn pTxn) = 0;

    virtual void setScratchSegment(
        SharedSegment pScratchSegment) = 0;

    virtual SharedLogicalTxn getTxn() = 0;
    
    virtual void addStream(
        SharedTupleStream pStream) = 0;

    virtual void addDataflow(
        TupleStreamId producerId,
        TupleStreamId consumerId) = 0;
    
    virtual void prepare() = 0;
    
    virtual void open() = 0;

    virtual uint getInputCount(
        TupleStreamId streamId) = 0;
    
    virtual SharedTupleStream getStreamInput(
        TupleStreamId streamId,
        uint iInput) = 0;

    /**
     * Get the sink of this graph; that is, the one stream which is not
     * consumed by any other stream.
     */
    virtual SharedTupleStream getSinkStream() = 0;

    static SharedTupleStreamGraph newTupleStreamGraph();
};

FENNEL_END_NAMESPACE

#endif

// End TupleStreamGraph.h
