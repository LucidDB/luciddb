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

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/TupleStreamBuilder.h"
#include "fennel/db/Database.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleStreamBuilder::TupleStreamBuilder(
    SharedDatabase pDatabaseInit,
    ExecutionStreamFactory &streamFactoryInit,
    SharedExecutionStreamGraph pGraphInit)
    : streamFactory(streamFactoryInit)
{
    pDatabase = pDatabaseInit;
    pGraph = pGraphInit;
}

void TupleStreamBuilder::buildStreamGraph(
    ProxyCmdPrepareExecutionStreamGraph &cmd)
{
    SegmentAccessor scratchAccessor =
        pDatabase->getSegmentFactory()->newScratchSegment(
            pDatabase->getCache());
    streamFactory.setScratchAccessor(scratchAccessor);
    pGraph->setScratchSegment(scratchAccessor.pSegment);

    // PASS 1: add streams to graph
    SharedProxyExecutionStreamDef pNext = cmd.getStreamDefs();
    for (; pNext; ++pNext) {
        visitStream(*pNext);
    }

    // PASS 2: add dataflows
    pNext = cmd.getStreamDefs();
    for (; pNext; ++pNext) {
        buildStreamInputs(*pNext);
        // streams with no consumer are responsible for providing buffers
        if (! pNext->getConsumer()) {
            std::string name = pNext->getName();
            SharedExecutionStream lastStream = pGraph->findLastStream(name);
            std::string baseName = lastStream->getName();
            addAdapterFor(name,baseName,TupleStream::PRODUCER_PROVISION);
        }
    }

    // PASS 3: sort and prepare streams
    pGraph->prepare();
    std::vector<SharedExecutionStream> sortedStreams = 
        pGraph->getSortedStreams();
    std::vector<SharedExecutionStream>::iterator pos;
    for (pos = sortedStreams.begin(); pos != sortedStreams.end(); pos++) {
        std::string name = (*pos)->getName();
        ExecutionStreamFactors factors = lookupStream(name);
        std::string traceName = getTraceName(name);
        factors.getStream()->initTraceSource(
            &(pDatabase->getTraceTarget()),
            traceName);
        factors.prepareStream();
    }
}

std::string TupleStreamBuilder::getTraceName(std::string streamName)
{
    return "xo." + streamName;
}

void TupleStreamBuilder::visitStream(
    ProxyExecutionStreamDef &streamDef)
{
    ExecutionStreamFactors factors = streamFactory.visitStream(streamDef);
    registerStream(factors);
    TraceTarget &traceTarget = pDatabase->getTraceTarget();
    std::string name = streamDef.getName();
    // add the XO prefix
    std::string traceName = getTraceName(name);
    if (traceTarget.getSourceTraceLevel(traceName) <= TRACE_FINE) {
        // interpose a tracing stream
        addTracingStream(name);
    }
}

void TupleStreamBuilder::buildStreamInputs(
    ProxyExecutionStreamDef &streamDef)
{
    std::string name = streamDef.getName();
    SharedExecutionStream pStream = pGraph->findStream(name);
    TupleStream::BufferProvision requiredDataflow =
        pStream->getInputBufferRequirement();
    SharedProxyExecutionStreamDef pInput = streamDef.getInput();
    for (; pInput; ++pInput) {
        std::string inputName = pInput->getName();
        SharedExecutionStream lastStream = pGraph->findLastStream(inputName);
        std::string baseName = lastStream->getName();
        addAdapterFor(inputName,baseName,requiredDataflow);
        addDataflow(inputName,name);
    }
}

void TupleStreamBuilder::addTracingStream(
    std::string &name)
{
    ExecutionStreamParams &params = lookupStream(name).getParams();
    std::string tracerName = name + ".tracer";
    ExecutionStreamFactors factors =
        streamFactory.newTracingStream(tracerName,params);
    registerStream(factors);

    // TracingStream may have different buffer provisioning requirements from
    // real stream, so may need adapters above and below.
    addAdapterFor(name,factors.getStream()->getInputBufferRequirement());
    interposeStream(name,factors.getStream()->getStreamId());
}

void TupleStreamBuilder::addAdapterFor(
    std::string &streamName,
    std::string &baseName,
    TupleStream::BufferProvision requiredDataflow)
{
    SharedExecutionStream pStream = pGraph->findLastStream(streamName);
    TupleStream::BufferProvision availableDataflow =
        pStream->getResultBufferProvision();
    assert(availableDataflow != TupleStream::NO_PROVISION);

    std::string adapterName = baseName + ".provisioner";
    switch (requiredDataflow) {
    case TupleStream::CONSUMER_PROVISION:
        if (availableDataflow == TupleStream::PRODUCER_PROVISION) {
            ExecutionStreamFactors factors =
                streamFactory.newProducerToConsumerProvisionAdapter(
                    adapterName,
                    lookupStream(streamName).getParams());
            registerStream(factors);
            interposeStream(streamName,factors.getStream()->getStreamId());
        }
        break;
    case TupleStream::PRODUCER_PROVISION:
        if (availableDataflow == TupleStream::CONSUMER_PROVISION) {
            ExecutionStreamFactors factors =
                streamFactory.newConsumerToProducerProvisionAdapter(
                    adapterName,
                    lookupStream(streamName).getParams());
            registerStream(factors);
            interposeStream(streamName,factors.getStream()->getStreamId());
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

void TupleStreamBuilder::addAdapterFor(
    std::string &streamName,
    TupleStream::BufferProvision requiredDataflow)
{
    addAdapterFor(streamName,streamName,requiredDataflow);
}

void TupleStreamBuilder::registerStream(ExecutionStreamFactors &factors)
{
    ExecutionStream *pNewStream = factors.getStream();
    std::string name = pNewStream->getName();
    streams[name] = factors;
    SharedExecutionStream pStream(pNewStream);
    pGraph->addStream(pStream);
}

ExecutionStreamFactors TupleStreamBuilder::lookupStream(std::string &name)
{
    StreamMapConstIter pPair = streams.find(name);
    assert(pPair != streams.end());
    return pPair->second;
}

void TupleStreamBuilder::addDataflow(
    std::string &source,
    std::string &target)
{
    SharedExecutionStream pInput = 
        pGraph->findLastStream(source);
    SharedExecutionStream pStream = 
        pGraph->findStream(target);
    pGraph->addDataflow(
        pInput->getStreamId(),
        pStream->getStreamId());
}
    
void TupleStreamBuilder::interposeStream(
    std::string &name,
    ExecutionStreamId interposedId)
{
    pGraph->interposeStream(
        name,
        interposedId);
}

FENNEL_END_CPPFILE("$Id$");

// End TupleStreamBuilder.cpp
