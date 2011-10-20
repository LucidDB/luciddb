/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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
#include "fennel/exec/ExecStreamExecutor.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TuplePrinter.h"
#include <fstream>

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamExecutor::ExecStreamExecutor(
    SharedTraceTarget target, std::string name)
    : TraceSource(target, name)
{
    tracingFine = isTracingLevel(TRACE_FINE);
}

ExecStreamExecutor::~ExecStreamExecutor()
{
}

// Summary of per-stream trace levels:
// TRACE_FINE: result of execution
// TRACE_FINER: buffer states before and after, output after execution.
// TRACE_FINEST: both input and output before and after each execution

void ExecStreamExecutor::tracePreExecution(
    ExecStream &stream,
    ExecStreamQuantum const &quantum)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "executing " << stream.getStreamId() << ' ' << stream.getName());
    if (!isMAXU(quantum.nTuplesMax)) {
        FENNEL_TRACE(
            TRACE_FINE,
            "nTuplesMax = " << quantum.nTuplesMax);
    }

    traceStreamBuffers(stream, TRACE_FINEST, TRACE_FINEST);
}

void ExecStreamExecutor::tracePostExecution(
    ExecStream &stream,
    ExecStreamResult rc)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "executed " << stream.getStreamId() << ' ' << stream.getName()
        << " with result " << ExecStreamResult_names[rc]);

    traceStreamBuffers(stream, TRACE_FINEST, TRACE_FINER);
}

void ExecStreamExecutor::traceStreamBuffers(
    ExecStream &stream,
    TraceLevel inputTupleTraceLevel,
    TraceLevel outputTupleTraceLevel)
{
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(stream.getGraph());
    ExecStreamGraphImpl::GraphRep const &graphRep = graphImpl.getGraphRep();

    ExecStreamGraphImpl::InEdgeIterPair inEdges =
        boost::in_edges(stream.getStreamId(), graphRep);
    for (uint i = 0; inEdges.first != inEdges.second;
         ++(inEdges.first),  ++i)
    {
        ExecStreamGraphImpl::Edge edge = *(inEdges.first);
        ExecStreamBufAccessor &bufAccessor =
            graphImpl.getBufAccessorFromEdge(edge);
        FENNEL_TRACE(
            TRACE_FINER,
            "input buffer " << i << ":  "
            << ExecStreamBufState_names[bufAccessor.getState()]
            << (bufAccessor.hasPendingEOS() ? ", EOS pending" : "")
            << ",  consumption available = "
            << bufAccessor.getConsumptionAvailable());
        if (stream.isTracingLevel(inputTupleTraceLevel)) {
            traceStreamBufferContents(
                stream, bufAccessor, inputTupleTraceLevel);
        }
    }

    ExecStreamGraphImpl::OutEdgeIterPair outEdges =
        boost::out_edges(stream.getStreamId(), graphRep);
    for (uint i = 0; outEdges.first != outEdges.second;
         ++(outEdges.first),  ++i)
    {
        ExecStreamGraphImpl::Edge edge = *(outEdges.first);
        ExecStreamBufAccessor &bufAccessor =
            graphImpl.getBufAccessorFromEdge(edge);
        FENNEL_TRACE(
            TRACE_FINER,
            "output buffer " << i << ":  "
            << ExecStreamBufState_names[bufAccessor.getState()]
            << (bufAccessor.hasPendingEOS() ? ", EOS pending" : "")
            << ",  consumption available = "
            << bufAccessor.getConsumptionAvailable()
            << ",  production available = "
            << bufAccessor.getProductionAvailable());
        if (stream.isTracingLevel(outputTupleTraceLevel)) {
            traceStreamBufferContents(
                stream, bufAccessor, outputTupleTraceLevel);
        }
    }
}

void ExecStreamExecutor::traceStreamBufferContents(
    ExecStream &stream,
    ExecStreamBufAccessor &bufAccessor,
    TraceLevel traceLevel)
{
    TupleDescriptor const &tupleDesc = bufAccessor.getTupleDesc();
    TupleData tupleData(tupleDesc);
    TupleAccessor &tupleAccessor = bufAccessor.getScratchTupleAccessor();

    for (PConstBuffer pTuple = bufAccessor.getConsumptionStart();
         pTuple != bufAccessor.getConsumptionEnd();
         pTuple += tupleAccessor.getCurrentByteCount())
    {
        tupleAccessor.setCurrentTupleBuf(pTuple);
        // while we're here, we might as well sanity-check the content
        assert(pTuple + tupleAccessor.getCurrentByteCount()
            <= bufAccessor.getConsumptionEnd());
        tupleAccessor.unmarshal(tupleData);
        // TODO:  sanity-check individual data values?
        std::ostringstream oss;
        TuplePrinter tuplePrinter;
        tuplePrinter.print(oss, tupleDesc, tupleData);
        stream.trace(traceLevel, oss.str());
    }
}

FENNEL_END_CPPFILE("$Id$");
// End ExecStreamExecutor.cpp
