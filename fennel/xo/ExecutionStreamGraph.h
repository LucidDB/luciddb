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

#ifndef Fennel_ExecutionStreamGraph_Included
#define Fennel_ExecutionStreamGraph_Included

#include "fennel/common/ClosableObject.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Identifier for a ExecutionStream relative to an instance of
 * ExecutionStreamGraph.
 */
typedef uint ExecutionStreamId;

/**
 * A ExecutionStreamGraph is a directed graph representing dataflow
 * among ExecutionStreams.
 */
template<class S>
class ExecutionStreamGraph : public boost::noncopyable, public ClosableObject
{
public:
    virtual ~ExecutionStreamGraph()
    {}

    virtual void setTxn(
        SharedLogicalTxn pTxn) = 0;

    virtual void setScratchSegment(
        SharedSegment pScratchSegment) = 0;

    virtual SharedLogicalTxn getTxn() = 0;
    
    virtual void addStream(
        S pStream) = 0;

    virtual void addDataflow(
        ExecutionStreamId producerId,
        ExecutionStreamId consumerId) = 0;
    
    virtual void prepare() = 0;
    
    virtual void open() = 0;

    virtual uint getInputCount(
        ExecutionStreamId streamId) = 0;
    
    virtual S getStreamInput(
        ExecutionStreamId streamId,
        uint iInput) = 0;

    /**
     * Get the sink of this graph; that is, the one stream which is not
     * consumed by any other stream.
     */
    virtual S getSinkStream() = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStreamGraph.h
