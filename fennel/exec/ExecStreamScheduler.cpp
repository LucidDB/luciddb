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
    : ExecStreamExecutor(pTraceTargetInit, nameInit)
{
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

void ExecStreamScheduler::restartStream(ExecStream &s)
{
    s.open(true);                       // by default, just re-open the target
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
