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
#include "fennel/farrago/TupleStreamBuilder.h"
#include "fennel/farrago/JavaTupleStream.h"
#include "fennel/farrago/CmdInterpreter.h"
#include "fennel/farrago/ExecutionStreamFactory.h"
#include "fennel/xo/BTreeScan.h"
#include "fennel/xo/BTreeSearch.h"
#include "fennel/xo/BTreeSearchUnique.h"
#include "fennel/xo/TableWriterStream.h"
#include "fennel/xo/BTreeLoader.h"
#include "fennel/xo/SortingStream.h"
#include "fennel/xo/BufferingTupleStream.h"
#include "fennel/xo/TracingTupleStream.h"
#include "fennel/xo/ProducerToConsumerProvisionAdapter.h"
#include "fennel/xo/ConsumerToProducerProvisionAdapter.h"
#include "fennel/xo/TableWriterFactory.h"
#include "fennel/xo/CartesianProductStream.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/cache/QuotaCacheAccessor.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleStreamBuilder::TupleStreamBuilder(
    SharedDatabase pDatabaseInit,
    ExecutionStreamFactory &streamFactoryInit,
    SharedTupleStreamGraph pGraphInit)
    : streamFactory(streamFactoryInit)
{
    pDatabase = pDatabaseInit;
    pGraph = pGraphInit;
}

void TupleStreamBuilder::buildStreamGraph(ProxyExecutionStreamDef &streamDef)
{
    SegmentAccessor scratchAccessor =
        pDatabase->getSegmentFactory()->newScratchSegment(
            pDatabase->getCache());
    streamFactory.setScratchAccessor(scratchAccessor);
    pGraph->setScratchSegment(scratchAccessor.pSegment);
    // perform a recursive traversal, requesting that the topmost stream
    // provide a buffer (since it has no consumer)
    visitStream(streamDef,TupleStream::PRODUCER_PROVISION);
    
    // the top-level stream visit should have left pChildStream set
    assert(pChildStream);
    // but we don't actually need it, since the graph remembers all the streams
    // just built
    pChildStream.reset();
}

void TupleStreamBuilder::visitStream(
    ProxyExecutionStreamDef &streamDef,
    TupleStream::BufferProvision requiredDataflow)
{
    TraceTarget &traceTarget = pDatabase->getTraceTarget();
    std::string name = streamDef.getName();

    // add the XO prefix
    name = "xo." + name;

    ExecutionStreamFactors factors = streamFactory.visitStream(streamDef);
    buildStreamInputs(factors.getStream(),streamDef);
    factors.prepareStream();
    childParams = static_cast<TupleStreamParams &>(factors.getParams());
    
    if (traceTarget.getSourceTraceLevel(name) <= TRACE_FINE) {
        // interpose a tracing stream
        addTracingStream(name);
    }

    addAdapterFor(requiredDataflow);
}

void TupleStreamBuilder::buildStreamInputs(
    ExecutionStream *pNewStream,ProxyExecutionStreamDef &streamDef)
{
    // One of the visit methods is in the middle of building a new stream.
    // First, add a reference to it to the graph.
    TupleStream *pTupleStream = static_cast<TupleStream*>(pNewStream);
    SharedTupleStream pStream(pTupleStream);
    pGraph->addStream(pStream);

    // Next, recursively visit each input, building depth-first
    SharedProxyExecutionStreamDef pInput = streamDef.getInput();
    for (; pInput; ++pInput) {
        visitStream(
            *pInput,
            pNewStream->getInputBufferRequirement());

        // visit should have left input subtree set in pChildStream
        assert(pChildStream);
        // record dataflow between input and pNewStream
        pGraph->addDataflow(
            pChildStream->getStreamId(),
            pStream->getStreamId());
        // forget input now that we're done with it; the graph remembers it
        pChildStream.reset();
    }
    // return this stream to parent once current visit completes
    pChildStream = pStream;
}

void TupleStreamBuilder::addAdapter(TupleStream &adapter)
{
    SharedTupleStream pSharedAdapter(&adapter);
    pGraph->addStream(pSharedAdapter);
    pGraph->addDataflow(
        pChildStream->getStreamId(),
        pSharedAdapter->getStreamId());
    pChildStream = pSharedAdapter;
    adapter.prepare(childParams);
}

void TupleStreamBuilder::addAdapterFor(
    TupleStream::BufferProvision requiredDataflow)
{
    TupleStream::BufferProvision availableDataflow =
        pChildStream->getResultBufferProvision();
    assert(availableDataflow != TupleStream::NO_PROVISION);
    switch (requiredDataflow) {
    case TupleStream::CONSUMER_PROVISION:
        if (availableDataflow == TupleStream::PRODUCER_PROVISION) {
            addAdapter(*new ProducerToConsumerProvisionAdapter());
        }
        break;
    case TupleStream::PRODUCER_PROVISION:
        if (availableDataflow == TupleStream::CONSUMER_PROVISION) {
            addAdapter(*new ConsumerToProducerProvisionAdapter());
        }
        break;
    case TupleStream::PRODUCER_OR_CONSUMER_PROVISION:
        // we can accept anything, so no adapter required
        break;
    default:
        assert(false);
        break;
    }
}

void TupleStreamBuilder::addTracingStream(std::string name)
{
    TraceTarget &traceTarget = pDatabase->getTraceTarget();
    TracingTupleStream *pTracingStream =
        new TracingTupleStream(traceTarget,name);
    SharedTupleStream pSharedTracingStream(pTracingStream);
    pGraph->addStream(pSharedTracingStream);

    // TracingStream may have different buffer provisioning requirements from
    // real stream, so may need adapters above and below.
    addAdapterFor(pTracingStream->getInputBufferRequirement());

    pGraph->addDataflow(
        pChildStream->getStreamId(),
        pSharedTracingStream->getStreamId());
    pChildStream = pSharedTracingStream;
    pTracingStream->prepare(childParams);
}

FENNEL_END_CPPFILE("$Id$");

// End TupleStreamBuilder.cpp
