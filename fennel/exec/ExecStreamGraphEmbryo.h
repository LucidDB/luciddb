/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#ifndef Fennel_ExecStreamGraphEmbryo_Included
#define Fennel_ExecStreamGraphEmbryo_Included

#include "fennel/exec/ExecStreamDefs.h"

#include <boost/utility.hpp>
#include <map>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamGraphEmbryo encapsulates the "embryonic" state of an
 * ExecStreamGraph as its constituent embryonic streams are built up.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ExecStreamGraphEmbryo : public boost::noncopyable
{
    typedef std::map<std::string,ExecStreamEmbryo> StreamMap;
    typedef StreamMap::const_iterator StreamMapConstIter;
    typedef StreamMap::iterator StreamMapIter;

    /**
     * Unprepared graph.
     */
    SharedExecStreamGraph pGraph;
    
    /**
     * Scheduler which will execute streams.
     */
    SharedExecStreamScheduler pScheduler;

    /**
     * Default cache accessor to be used by streams.
     */
    SharedCacheAccessor pCacheAccessor;

    /**
     * Default scratch segment accessor to be used by streams.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Streams to be linked and prepared, mapped by name
     */
    StreamMap allStreamEmbryos;

    /**
     * Initializes a new adapter stream.
     *
     * @param embryo embryo for new adapter
     *
     * @param streamName name of stream being adapted
     *
     * @param iOutput ordinal of the output within the stream being adapted
     *
     * @param adapterName name of adapter stream
     */
    void initializeAdapter(
        ExecStreamEmbryo &embryo,
        std::string const &streamName,
        uint iOutput,
        std::string const &adapterName);
    
public:
    explicit ExecStreamGraphEmbryo(
        SharedExecStreamGraph pGraph, 
        SharedExecStreamScheduler pScheduler,
        SharedCache pCache,
        SharedSegmentFactory pSegmentFactory);

    virtual ~ExecStreamGraphEmbryo();

    /**
     * Initializes a stream's generic parameters.
     *
     * @param params parameters to be initialized
     */
    void initStreamParams(ExecStreamParams &params);

    /**
     * @return graph being built
     */
    ExecStreamGraph &getGraph();

    /**
     * @return accessor for graph's scratch segment
     */
    SegmentAccessor &getScratchAccessor();

    /**
     * Ensures that a producer is capable of the specified buffer 
     * provisioning requirements. If producer is not capable, an adapter
     * stream is appended to supply the required buffer provisioning. 
     *
     * <p>The "producer" may be a single stream or may be a chain of 
     * streams. In either case, the adapter is appended to the end of the 
     * group under the name of the original stream. It is named according
     * to the last stream:
     * <code><i>lastName</i>#<i>iOutput</i>.provisioner</code>
     *
     * @param name name of original stream
     *
     * @param iOutput ordinal of the output within the producer
     *
     * @param requiredDataFlow buffer provisioning requirement
     *
     * @return adapted stream
     */
    SharedExecStream addAdapterFor(
        const std::string &name,
        uint iOutput,
        ExecStreamBufProvision requiredDataFlow);

    /**
     * Registers a newly created, unprepared stream and adds it to the graph.
     *
     * @param embryo stream embryo
     */
    void saveStreamEmbryo(
        ExecStreamEmbryo &embryo);
    
    /**
     * Looks up a registered stream. The stream *must* already be registered.
     *
     * @param name of stream to find
     *
     * @return corresponding embryo
     */
    ExecStreamEmbryo &getStreamEmbryo(
        const std::string &name);
    
    /**
     * Adds dataflow to graph, from one stream's output, after adapters, to
     * another stream.
     *
     * @param source name of source stream
     *
     * @param target name of target stream
     *
     * @param isImplicit false (the default) if the edge represents
     * direct dataflow; true if the edge represents an implicit
     * dataflow dependency
     */
    void addDataflow(
        const std::string &source,
        const std::string &target,
        bool isImplicit = false);

    /**
     * Prepares graph and all of its streams.
     *
     * @param pTraceTarget trace target for stream execution
     *
     * @param tracePrefix common prefix for stream trace names
     */
    void prepareGraph(
        SharedTraceTarget pTraceTarget,
        std::string const &tracePrefix);
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamGraphEmbryo.h
