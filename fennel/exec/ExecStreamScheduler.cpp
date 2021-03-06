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
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/CopyExecStream.h"
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TuplePrinter.h"

#include <fstream>

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamScheduler::ExecStreamScheduler(
    SharedTraceTarget pTraceTargetInit,
    std::string nameInit)
    : TraceSource(pTraceTargetInit, nameInit)
{
    tracingFine = isTracingLevel(TRACE_FINE);
}

ExecStreamScheduler::~ExecStreamScheduler()
{
}

void ExecStreamScheduler::addGraph(SharedExecStreamGraph pGraph)
{
    assert(!pGraph->pScheduler);
    pGraph->pScheduler = this;

    if (tracingFine) {
        std::string dotFileName;
        const char *fennelHome = getenv("FENNEL_HOME");
        if (fennelHome) {
            dotFileName += fennelHome;
            dotFileName += "/trace/";
        }
        dotFileName += "ExecStreamGraph.dot";
        std::ofstream dotStream(dotFileName.c_str());
        pGraph->renderGraphviz(dotStream);
    }

    // if any of the streams in the new graph require tracing, then
    // disable our tracing short-circuit
    std::vector<SharedExecStream> streams = pGraph->getSortedStreams();
    for (uint i = 0; i < streams.size(); ++i) {
        if (streams[i]->isTracingLevel(TRACE_FINE)) {
            tracingFine = true;
            return;
        }
    }
}

SharedExecStreamBufAccessor ExecStreamScheduler::newBufAccessor()
{
    return SharedExecStreamBufAccessor(new ExecStreamBufAccessor());
}

void ExecStreamScheduler::createBufferProvisionAdapter(
    ExecStreamEmbryo &embryo)
{
    ScratchBufferExecStreamParams adapterParams;
    embryo.init(
        new ScratchBufferExecStream(),
        adapterParams);
}

void ExecStreamScheduler::createCopyProvisionAdapter(
    ExecStreamEmbryo &embryo)
{
    CopyExecStreamParams adapterParams;
    embryo.init(
        new CopyExecStream(),
        adapterParams);
}

void ExecStreamScheduler::removeGraph(SharedExecStreamGraph pGraph)
{
    assert(pGraph->pScheduler == this);
    pGraph->pScheduler = NULL;
}

// Summary of per-stream trace levels:
// TRACE_FINE: result of execution
// TRACE_FINER: buffer states before and after, output after execution.
// TRACE_FINEST: both input and output before and after each execution

void ExecStreamScheduler::tracePreExecution(
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

void ExecStreamScheduler::tracePostExecution(
    ExecStream &stream,
    ExecStreamResult rc)
{
    FENNEL_TRACE(
        TRACE_FINE,
        "executed " << stream.getStreamId() << ' ' << stream.getName()
        << " with result " << ExecStreamResult_names[rc]);

    traceStreamBuffers(stream, TRACE_FINEST, TRACE_FINER);
}

void ExecStreamScheduler::traceStreamBuffers(
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

// An abstract functor applied to a const row in a const stream buffer.
class ExecStreamBufTupleFunctor
{
public:
    virtual ~ExecStreamBufTupleFunctor() {}

    // applies the functor to a row,
    // represented as a pair (TupleDescriptor, TupleData).
    virtual void operator()(const TupleDescriptor&, const TupleData&) {}

    // applies the functor to the next CT unread rows in the buffer
    void foreachUnreadRow(ExecStreamBufAccessor&, int ct = INT_MAX);

    // applies the functor to each row in a buffer range,
    // but no more than CT rows
    void foreachRow(
        ExecStreamBufAccessor&,
        PConstBuffer start, PConstBuffer end, int ct = INT_MAX);
};

void ExecStreamBufTupleFunctor::foreachUnreadRow(
    ExecStreamBufAccessor& bufAccessor, int ct)
{
    PConstBuffer start = bufAccessor.getConsumptionStart();
    PConstBuffer end =  bufAccessor.getConsumptionEnd();
    foreachRow(bufAccessor, start, end, ct);
}

// apply to each row in buffer from START to END, but stop after CT rows
void ExecStreamBufTupleFunctor::foreachRow(
    ExecStreamBufAccessor &bufAccessor,
    PConstBuffer start, PConstBuffer end, int ct)
{
    TupleDescriptor const &tupleDesc = bufAccessor.getTupleDesc();
    TupleData tupleData(tupleDesc);
    TupleAccessor &tupleAccessor = bufAccessor.getScratchTupleAccessor();

    for (PConstBuffer pTuple = start;
         ct > 0 && pTuple != end;
         ct--, pTuple += tupleAccessor.getCurrentByteCount())
    {
        tupleAccessor.setCurrentTupleBuf(pTuple);
        // while we're here, we might as well sanity-check the content
        assert(pTuple + tupleAccessor.getCurrentByteCount()
            <= bufAccessor.getConsumptionEnd());
        tupleAccessor.unmarshal(tupleData);
        // TODO:  sanity-check individual data values?
        (*this)(tupleDesc, tupleData);
    }
}

// a functor that prints a row to an ostream
class PrintTupleFunctor : public ExecStreamBufTupleFunctor
{
    std::ostream& os;
    bool endline;
public:
    PrintTupleFunctor(std::ostream& os, bool endline = false)
        : os(os), endline(endline) {}
    virtual void operator()(const TupleDescriptor&, const TupleData&);
};

void PrintTupleFunctor::operator()
    (const TupleDescriptor& desc, const TupleData& data)
{
    TuplePrinter tuplePrinter;
    tuplePrinter.print(os, desc, data);
    if (endline) {
        os << std::endl;
    }
}

// a functor that trace a row
class TraceTupleFunctor : public ExecStreamBufTupleFunctor
{
    TraceSource& traceSource;
    TraceLevel traceLevel;
public:
    TraceTupleFunctor(TraceSource& s, TraceLevel n)
        : traceSource(s), traceLevel(n) {}
    virtual void operator()(const TupleDescriptor&, const TupleData&);
};

void TraceTupleFunctor::operator()
    (const TupleDescriptor& desc, const TupleData& data)
{
    std::ostringstream oss;
    TuplePrinter tuplePrinter;
    tuplePrinter.print(oss, desc, data);
    traceSource.trace(traceLevel, oss.str());
}

// public methods
void ExecStreamScheduler::printStreamBufferContents(
    std::ostream& os,
    ExecStreamBufAccessor &bufAccessor)
{
    PrintTupleFunctor f(os, true);
    f.foreachUnreadRow(bufAccessor);
}

void ExecStreamScheduler::traceStreamBufferContents(
    ExecStream &stream,
    ExecStreamBufAccessor &bufAccessor,
    TraceLevel traceLevel)
{
    TraceTupleFunctor f(stream, traceLevel);
    f.foreachUnreadRow(bufAccessor);
}

void ExecStreamScheduler::checkAbort() const
{
}

uint ExecStreamScheduler::getDegreeOfParallelism()
{
    return 1;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamScheduler.cpp
