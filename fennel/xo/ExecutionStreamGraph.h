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

#ifndef Fennel_ExecutionStreamGraph_Included
#define Fennel_ExecutionStreamGraph_Included

#include "fennel/common/ClosableObject.h"

#include <vector>
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
 *
 * <p>
 *
 * When a stream is added to a graph is assigned an ExecutionStreamId. The
 * identifier is later used to work with the stream.
 *
 * <p>
 * 
 * In addition, streams are required to have unique names when they are added
 * to the graph. These names are used to find the streams by name. Because
 * tracing streams and adapters may replace a stream's output, the graph also 
 * finds the last stream for a given name.
 *
 * The graph can retrieve streams by these names. In order to
 * support tracing streams and adapters and tracing streams
 * are sometimes interposed behind regular streams.
 */
class ExecutionStreamGraph : public boost::noncopyable, public ClosableObject
{
public:
    virtual ~ExecutionStreamGraph();

    virtual void setTxn(
        SharedLogicalTxn pTxn) = 0;

    virtual void setScratchSegment(
        SharedSegment pScratchSegment) = 0;

    virtual SharedLogicalTxn getTxn() = 0;
    
    virtual void addStream(
        SharedExecutionStream pStream) = 0;

    virtual void addDataflow(
        ExecutionStreamId producerId,
        ExecutionStreamId consumerId) = 0;

    virtual void removeStream(
        SharedExecutionStream pStream) 
    {
        assert(false);
    }
        
    /**
     * Find a stream by name
     */
    virtual SharedExecutionStream findStream(
            std::string name) = 0;
    
    /**
     * Find last stream for name. May be original stream or an adapter.
     */
    virtual SharedExecutionStream findLastStream(
            std::string name) = 0;
    
    /**
     * Replace last stream for name. In the process, creates a dataflow 
     * from last stream to it's replacement.
     */
    virtual void interposeStream(
        std::string name,
        ExecutionStreamId interposedId) = 0;

    virtual void prepare() = 0;
    
    virtual void open() = 0;

    virtual uint getInputCount(
        ExecutionStreamId streamId) = 0;
    
    virtual SharedExecutionStream getStreamInput(
        ExecutionStreamId streamId,
        uint iInput) = 0;

    /**
     * Get the sink of this graph; that is, the one stream which is not
     * consumed by any other stream.
     */
    virtual SharedExecutionStream getSinkStream() = 0;

    /**
     * Get streams, sorted topologically. Can only be called after prepare.
     */
    virtual std::vector<SharedExecutionStream> getSortedStreams() = 0;

    /**
     * Workaround for multiple inheritance. Get pointer to this graph, 
     * casted as it's ultimate interface. Useful when virtual inheritance
     * prohibits static casting. 
     */
    virtual void *getInterface() =  0;

    /**
     * Workaround for multiple inheritance. Get name of ultimate interface.
     */
    virtual char *getInterfaceName() =  0;
};

FENNEL_END_NAMESPACE

#endif

// End ExecutionStreamGraph.h
