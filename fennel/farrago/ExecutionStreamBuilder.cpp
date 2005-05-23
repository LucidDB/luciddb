/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/ExecutionStreamBuilder.h"
#include "fennel/db/Database.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecutionStreamBuilder::ExecutionStreamBuilder(
    SharedDatabase pDatabaseInit,
    ExecutionStreamFactory &streamFactoryInit,
    SharedExecutionStreamGraph pGraphInit)
    : streamFactory(streamFactoryInit)
{
    pDatabase = pDatabaseInit;
    pGraph = pGraphInit;
}

void ExecutionStreamBuilder::buildStreamGraph(
    ProxyCmdPrepareExecutionStreamGraph &cmd)
{
    // Allocate scratch segment for use by graph and streams
    SegmentAccessor scratchAccessor =
        pDatabase->getSegmentFactory()->newScratchSegment(
            pDatabase->getCache());
    streamFactory.setScratchAccessor(scratchAccessor);
    pGraph->setScratchSegment(scratchAccessor.pSegment);

    // PASS 1: add streams to graph
    SharedProxyExecutionStreamDef pNext = cmd.getStreamDefs();
    for (; pNext; ++pNext) {
        buildStream(*pNext);
    }

    // PASS 2: add dataflows
    pNext = cmd.getStreamDefs();
    for (; pNext; ++pNext) {
        buildStreamInputs(*pNext);
        // Streams with no consumer are read directly by clients.  They 
        // are expected to support producer provisioned results.
        if (! pNext->getConsumer()) {
            std::string name = pNext->getName();
            addAdapterFor(name, ExecutionStream::PRODUCER_PROVISION);
        }
    }

    // PASS 3: sort and prepare streams
    pGraph->prepare();
    std::vector<SharedExecutionStream> sortedStreams = 
        pGraph->getSortedStreams();
    std::vector<SharedExecutionStream>::iterator pos;
    for (pos = sortedStreams.begin(); pos != sortedStreams.end(); pos++) {
        std::string name = (*pos)->getName();
        ExecutionStreamParts parts = getStreamParts(name);
        std::string traceName = getTraceName(name);
        parts.getStream()->initTraceSource(
            &(pDatabase->getTraceTarget()),
            traceName);
        parts.prepareStream();
    }
}

std::string ExecutionStreamBuilder::getTraceName(
    const std::string &streamName)
{
    // Give streams a source name with an XO prefix so that users can 
    // choose to trace XOs as a group
    return "xo." + streamName;
}

void ExecutionStreamBuilder::buildStream(
    ProxyExecutionStreamDef &streamDef)
{
    ExecutionStreamParts parts = streamFactory.visitStream(streamDef);
    saveStreamParts(parts);

    // Appends a tracing stream if the tracing level is high enough
    TraceTarget &traceTarget = pDatabase->getTraceTarget();
    std::string name = streamDef.getName();
    if (traceTarget.getSourceTraceLevel(getTraceName(name)) <= TRACE_FINE) {
        addTracingStream(name);
    }
}

void ExecutionStreamBuilder::buildStreamInputs(
    ProxyExecutionStreamDef &streamDef)
{
    std::string name = streamDef.getName();
    SharedExecutionStream pStream = pGraph->findStream(name);
    ExecutionStream::BufferProvision requiredDataflow =
        pStream->getInputBufferRequirement();
    SharedProxyExecutionStreamDef pInput = streamDef.getInput();
    for (; pInput; ++pInput) {
        std::string inputName = pInput->getName();
        addAdapterFor(inputName, requiredDataflow);
        addDataflow(inputName, name);
    }
}

void ExecutionStreamBuilder::addTracingStream(
    const std::string &name)
{
    ExecutionStreamParts parts = getStreamParts(name);
    if (parts.getTraceType() == ExecutionStreamParts::TRACE_TYPE_NONE) {
        return;
    }

    // Create a tracing stream based on the original stream
    std::string tracerName = name + ".tracer";
    ExecutionStreamParts tracerParts =
        streamFactory.newTracingStream(tracerName, parts);
    saveStreamParts(tracerParts);

    // Ensure the original stream supports the buffer provision required 
    // by the tracing stream and append the tracing stream
    addAdapterFor(name, tracerParts.getStream()->getInputBufferRequirement());
    interposeStream(name, tracerParts.getStream()->getStreamId());
}

void ExecutionStreamBuilder::addAdapterFor(
    const std::string &name,
    ExecutionStream::BufferProvision requiredDataflow)
{
    // Get available dataflow from last stream of group
    SharedExecutionStream pLastStream = pGraph->findLastStream(name);
    ExecutionStream::BufferProvision availableDataflow =
        pLastStream->getResultBufferProvision();
    assert(availableDataflow != ExecutionStream::NO_PROVISION);

    // If necessary, create an adapter based on the last stream
    std::string adapterName = pLastStream->getName() + ".provisioner";
    switch (requiredDataflow) {
    case ExecutionStream::CONSUMER_PROVISION:
        if (availableDataflow == ExecutionStream::PRODUCER_PROVISION) {
            ExecutionStreamParts parts =
                streamFactory.newProducerToConsumerProvisionAdapter(
                    adapterName,
                    getStreamParts(pLastStream->getName()).getParams());
            saveStreamParts(parts);
            interposeStream(name, parts.getStream()->getStreamId());
        }
        break;
    case ExecutionStream::PRODUCER_PROVISION:
        if (availableDataflow == ExecutionStream::CONSUMER_PROVISION) {
            ExecutionStreamParts parts =
                streamFactory.newConsumerToProducerProvisionAdapter(
                    adapterName,
                    getStreamParts(pLastStream->getName()).getParams());
            saveStreamParts(parts);
            interposeStream(name, parts.getStream()->getStreamId());
        }
        break;
    case ExecutionStream::PRODUCER_OR_CONSUMER_PROVISION:
        // we can accept anything, so no adapter required
        break;
    default:
        permAssert(false);
    }
}

void ExecutionStreamBuilder::saveStreamParts(ExecutionStreamParts &parts)
{
    ExecutionStream *pNewStream = parts.getStream();
    allStreamParts[pNewStream->getName()] = parts;
    pGraph->addStream(SharedExecutionStream(pNewStream));
}

ExecutionStreamParts ExecutionStreamBuilder::getStreamParts(
    std::string const &name)
{
    StreamMapConstIter pPair = allStreamParts.find(name);
    assert(pPair != allStreamParts.end());
    return pPair->second;
}

void ExecutionStreamBuilder::addDataflow(
    const std::string &source,
    const std::string &target)
{
    SharedExecutionStream pInput = 
        pGraph->findLastStream(source);
    SharedExecutionStream pStream = 
        pGraph->findStream(target);
    pGraph->addDataflow(
        pInput->getStreamId(),
        pStream->getStreamId());
}
    
void ExecutionStreamBuilder::interposeStream(
    const std::string &name,
    ExecutionStreamId interposedId)
{
    pGraph->interposeStream(
        name,
        interposedId);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecutionStreamBuilder.cpp
