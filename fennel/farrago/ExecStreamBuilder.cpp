/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/ExecStreamBuilder.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStream.h"
#include "fennel/db/Database.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamBuilder::ExecStreamBuilder(
    ExecStreamGraphEmbryo &graphEmbryoInit,
    ExecStreamFactory &streamFactoryInit)
    : graphEmbryo(graphEmbryoInit),
      streamFactory(streamFactoryInit)
{
}

ExecStreamBuilder::~ExecStreamBuilder()
{
}

void ExecStreamBuilder::buildStreamGraph(
    ProxyCmdPrepareExecutionStreamGraph &cmd,
    bool assumeOutputFromSinks)
{
    streamFactory.setScratchAccessor(graphEmbryo.getScratchAccessor());

    // PASS 1: add streams to graph
    SharedProxyExecutionStreamDef pStreamDef = cmd.getStreamDefs();
    for (; pStreamDef; ++pStreamDef) {
        buildStream(*pStreamDef);
    }

    // PASS 2: add input dataflows (provided the source input has only output)
    pStreamDef = cmd.getStreamDefs();
    for (; pStreamDef; ++pStreamDef) {
        buildStreamInputs(*pStreamDef);

        if (!getExplicitOutputCount(*pStreamDef) && assumeOutputFromSinks) {
            // Streams with no consumer are read directly by clients.  They
            // are expected to support producer provisioned results.
            std::string name = pStreamDef->getName();
            SharedExecStream pAdaptedStream =
                graphEmbryo.addAdapterFor(name, 0, BUFPROV_PRODUCER);
            graphEmbryo.getGraph().addOutputDataflow(
                pAdaptedStream->getStreamId());
        }
    }

    // PASS 3: add output dataflows in the cases where a stream has multiple
    // outputs
    pStreamDef = cmd.getStreamDefs();
    for (; pStreamDef; ++pStreamDef) {
        buildStreamOutputs(*pStreamDef);
    }

    // PASS 4: sort and prepare streams
    graphEmbryo.prepareGraph(
        streamFactory.getDatabase()->getSharedTraceTarget(),
        "xo.");
}

void ExecStreamBuilder::buildStream(
    ProxyExecutionStreamDef &streamDef)
{
    ExecStreamEmbryo embryo = streamFactory.visitStream(streamDef);
    graphEmbryo.saveStreamEmbryo(embryo);
    SharedProxyDynamicParamUse pParamUse = streamDef.getDynamicParamUse();
    for (; pParamUse; ++pParamUse) {
        DynamicParamId dynamicParamId(pParamUse->getDynamicParamId());
        if (pParamUse->isRead()) {
            if (false)
                std::cout << "stream " << embryo.getStream()->getStreamId()
                          << " reads param " << dynamicParamId << std::endl;
            graphEmbryo.getGraph().declareDynamicParamReader(
                embryo.getStream()->getStreamId(),
                dynamicParamId);
        } else {
            if (false)
                std::cout << "stream " << embryo.getStream()->getStreamId()
                          << " writes param " << dynamicParamId << std::endl;
            graphEmbryo.getGraph().declareDynamicParamWriter(
                embryo.getStream()->getStreamId(),
                dynamicParamId);
        }
    }
}

void ExecStreamBuilder::buildStreamInputs(
    ProxyExecutionStreamDef &streamDef)
{
    std::string name = streamDef.getName();
    SharedProxyExecStreamDataFlow pInputFlow = streamDef.getInputFlow();
    for (; pInputFlow; ++pInputFlow) {
        SharedProxyExecutionStreamDef pInput = pInputFlow->getProducer();
        // If the source input has multiple outputs, defer adding that flow
        // till later so we can add those flows in the order in which they
        // appear in the output flow list.
        //
        // NOTE zfong 12/4/06 - By deferring adding the input flows in the
        // scenario described above, this means we don't handle the case where
        // a dataflow is an ordered dataflow for both an input and an output.
        // The ordering will only be preserved on the output flows.
        if (getExplicitOutputCount(*pInput) > 1) {
            continue;
        }
        std::string inputName = pInput->getName();
        graphEmbryo.addDataflow(inputName, name, pInputFlow->isImplicit());
    }
}

void ExecStreamBuilder::buildStreamOutputs(
    ProxyExecutionStreamDef &streamDef)
{
    std::string name = streamDef.getName();
    SharedProxyExecStreamDataFlow pOutputFlow = streamDef.getOutputFlow();
    if (!(getExplicitOutputCount(streamDef) > 1)) {
        return;
    }
    for (; pOutputFlow; ++pOutputFlow) {
        SharedProxyExecutionStreamDef pOutput = pOutputFlow->getConsumer();
        std::string outputName = pOutput->getName();
        graphEmbryo.addDataflow(name, outputName, pOutputFlow->isImplicit());
    }
}

int ExecStreamBuilder::getExplicitOutputCount(
    ProxyExecutionStreamDef &streamDef)
{
    int nExplicitOutputs = 0;
    SharedProxyExecStreamDataFlow pOutputFlow = streamDef.getOutputFlow();
    for (; pOutputFlow; ++pOutputFlow) {
        if (!pOutputFlow->isImplicit()) {
            ++nExplicitOutputs;
        }
    }
    return nExplicitOutputs;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamBuilder.cpp
