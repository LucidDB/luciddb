/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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

    // PASS 2: add dataflows
    pStreamDef = cmd.getStreamDefs();
    for (; pStreamDef; ++pStreamDef) {
        buildStreamInputs(*pStreamDef);
        
        if (!pStreamDef->getConsumer() && assumeOutputFromSinks) {
            // Streams with no consumer are read directly by clients.  They 
            // are expected to support producer provisioned results.
            std::string name = pStreamDef->getName();
            SharedExecStream pAdaptedStream =
                graphEmbryo.addAdapterFor(name, 0, BUFPROV_PRODUCER);
            graphEmbryo.getGraph().addOutputDataflow(
                pAdaptedStream->getStreamId());
        }
    }

    // PASS 3: sort and prepare streams
    graphEmbryo.prepareGraph(
        &(streamFactory.getDatabase()->getTraceTarget()),
        "xo.");
}

void ExecStreamBuilder::buildStream(
    ProxyExecutionStreamDef &streamDef)
{
    ExecStreamEmbryo embryo = streamFactory.visitStream(streamDef);
    graphEmbryo.saveStreamEmbryo(embryo);
}

void ExecStreamBuilder::buildStreamInputs(
    ProxyExecutionStreamDef &streamDef)
{
    std::string name = streamDef.getName();
    SharedProxyExecutionStreamDef pInput = streamDef.getInput();
    for (; pInput; ++pInput) {
        std::string inputName = pInput->getName();
        graphEmbryo.addDataflow(inputName, name);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamBuilder.cpp
