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
#include "fennel/farrago/ExecStreamBuilder.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/db/Database.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamBuilder::ExecStreamBuilder(
    SharedDatabase pDatabaseInit,
    SharedExecStreamScheduler pSchedulerInit,
    ExecStreamFactory &streamFactoryInit,
    SharedExecStreamGraph pGraphInit)
    : streamFactory(streamFactoryInit)
{
    pDatabase = pDatabaseInit;
    pScheduler = pSchedulerInit;
    pGraph = pGraphInit;
}

ExecStreamBuilder::~ExecStreamBuilder()
{
}

void ExecStreamBuilder::buildStreamGraph(
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
            SharedExecStream pAdaptedStream =
                addAdapterFor(name, BUFPROV_PRODUCER);
            pGraph->addOutputDataflow(pAdaptedStream->getStreamId());
        }
    }

    // PASS 3: sort and prepare streams
    pGraph->prepare(*pScheduler);
    std::vector<SharedExecStream> sortedStreams = 
        pGraph->getSortedStreams();
    std::vector<SharedExecStream>::iterator pos;
    for (pos = sortedStreams.begin(); pos != sortedStreams.end(); pos++) {
        std::string name = (*pos)->getName();
        ExecStreamEmbryo embryo = getStreamEmbryo(name);
        std::string traceName = getTraceName(name);
        embryo.getStream()->initTraceSource(
            &(pDatabase->getTraceTarget()),
            traceName);
        embryo.prepareStream();
    }
}

std::string ExecStreamBuilder::getTraceName(
    const std::string &streamName)
{
    // Give streams a source name with an XO prefix so that users can 
    // choose to trace XOs as a group
    return "xo." + streamName;
}

void ExecStreamBuilder::buildStream(
    ProxyExecutionStreamDef &streamDef)
{
    ExecStreamEmbryo embryo = streamFactory.visitStream(streamDef);
    saveStreamEmbryo(embryo);
}

void ExecStreamBuilder::buildStreamInputs(
    ProxyExecutionStreamDef &streamDef)
{
    std::string name = streamDef.getName();
    SharedExecStream pStream = pGraph->findStream(name);
    ExecStreamBufProvision requiredDataflow =
        pStream->getInputBufProvision();
    SharedProxyExecutionStreamDef pInput = streamDef.getInput();
    for (; pInput; ++pInput) {
        std::string inputName = pInput->getName();
        addAdapterFor(inputName, requiredDataflow);
        addDataflow(inputName, name);
    }
}

SharedExecStream ExecStreamBuilder::addAdapterFor(
    const std::string &name,
    ExecStreamBufProvision requiredDataflow)
{
    // Get available dataflow from last stream of group
    SharedExecStream pLastStream = pGraph->findLastStream(name);
    ExecStreamBufProvision availableDataflow =
        pLastStream->getOutputBufProvision();
    assert(availableDataflow != BUFPROV_NONE);

    // If necessary, create an adapter based on the last stream
    std::string adapterName = pLastStream->getName() + ".provisioner";
    switch (requiredDataflow) {
    case BUFPROV_CONSUMER:
        if (availableDataflow == BUFPROV_PRODUCER) {
            ExecStreamEmbryo embryo =
                streamFactory.newProducerToConsumerProvisionAdapter(
                    adapterName,
                    *(getStreamEmbryo(pLastStream->getName()).getParams()));
            saveStreamEmbryo(embryo);
            interposeStream(name, embryo.getStream()->getStreamId());
            return embryo.getStream();
        }
        break;
    case BUFPROV_PRODUCER:
        if (availableDataflow == BUFPROV_CONSUMER) {
            ExecStreamEmbryo embryo =
                streamFactory.newConsumerToProducerProvisionAdapter(
                    adapterName,
                    *(getStreamEmbryo(pLastStream->getName()).getParams()));
            saveStreamEmbryo(embryo);
            interposeStream(name, embryo.getStream()->getStreamId());
            return embryo.getStream();
        }
        break;
    default:
        permAssert(false);
    }
    return pLastStream;
}

void ExecStreamBuilder::saveStreamEmbryo(ExecStreamEmbryo &embryo)
{
    allStreamEmbryos[embryo.getStream()->getName()] = embryo;
    pGraph->addStream(embryo.getStream());
}

ExecStreamEmbryo ExecStreamBuilder::getStreamEmbryo(
    std::string const &name)
{
    StreamMapConstIter pPair = allStreamEmbryos.find(name);
    assert(pPair != allStreamEmbryos.end());
    return pPair->second;
}

void ExecStreamBuilder::addDataflow(
    const std::string &source,
    const std::string &target)
{
    SharedExecStream pInput = 
        pGraph->findLastStream(source);
    SharedExecStream pStream = 
        pGraph->findStream(target);
    pGraph->addDataflow(
        pInput->getStreamId(),
        pStream->getStreamId());
}
    
void ExecStreamBuilder::interposeStream(
    const std::string &name,
    ExecStreamId interposedId)
{
    pGraph->interposeStream(
        name,
        interposedId);
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamBuilder.cpp
