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

#ifndef Fennel_ExecStreamBuilder_Included
#define Fennel_ExecStreamBuilder_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/farrago/ExecStreamFactory.h"
#include "fennel/farrago/Fem.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraphEmbryo.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamBuilder builds a prepared ExecStreamGraph from its 
 * Java representation.  It builds a graph in three phases:
 *
 * <ul>
 *   <li>First, it builds the streams</li>
 *   <li>Then, it builds the dataflows</li>
 *   <li>Lastly, it prepares the graph and streams</li>
 * </ul>
 *
 * <p><b>Cache.</b>
 * A new scratch segment is allocated by the builder and is shared between 
 * the graph and its streams.
 *
 * <p><b>Tracing.</b>
 *   All streams are assigned a trace 
 *     name of: <code>xo.<i>streamName</i></code>  Depending on a the 
 *     TraceTarget, this typically corresponds to a trace property like 
 *     <code>org.eigenbase.fennel.xo.<i>streamName</i></code>
 *
 * <p><b>Buffer Provisioning.</b>
 * Provisioning adapters are special streams interposed between two other 
 * streams when the producer's result provisioning does not meet the 
 * consumer's input requirements.  They are interposed during the dataflow 
 * phase.  They are named: <code><i>producerName</i>.provisioner</code>
 *
 * <p><b>Interposition.</b>
 * When provisioning adapters are appended to a stream, they 
 * consume the original stream's output and produce a new output.  To make 
 * the appended streams work transparently, the chain of streams is 
 * treated as a single unit.  Subsequent access to the stream's output is 
 * available through the graph by finding the "last" stream registered 
 * under the original stream's name.
 */
class ExecStreamBuilder : public boost::noncopyable
{
    /**
     * Embryo for graph being built up.
     */
    ExecStreamGraphEmbryo &graphEmbryo;
    
    /**
     * Factory for creating ExecStream objects.
     */
    ExecStreamFactory &streamFactory;
    
    /**
     * Allocates a stream based on stream definition, adds the stream to a 
     * graph and records how to prepare the stream.
     */
    void buildStream(
        ProxyExecutionStreamDef &);

    /**
     * Adds dataflows between a stream and its inputs. Interposes
     * provisioning adapters as required.
     *
     * @param streamDef corresponding Java stream definition being converted
     */
    void buildStreamInputs(
        ProxyExecutionStreamDef &streamDef);

public:
    /**
     * Creates a new ExecStreamBuilder.
     *
     * @param graphEmbryo embryo for graph to be built
     *
     * @param streamFactory factory for creating streams
     */
    explicit ExecStreamBuilder(
        ExecStreamGraphEmbryo &graphEmbryo, 
        ExecStreamFactory &streamFactory);

    virtual ~ExecStreamBuilder();

    /**
     * Main builder entry point.
     *
     * @param streamDef Java representation for collection of stream
     * definitions
     */
    void buildStreamGraph(ProxyCmdPrepareExecutionStreamGraph &cmd);
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamBuilder.h
