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

#ifndef Fennel_DfsTreeExecStreamScheduler_Included
#define Fennel_DfsTreeExecStreamScheduler_Included

#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraphImpl.h"

FENNEL_BEGIN_NAMESPACE

class ExecStreamGraphImpl;

/**
 * DfsTreeExecStreamScheduler is a reference implementation of
 * the ExecStreamScheduler interface.
 * See SchedulerDesign for more details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT DfsTreeExecStreamScheduler
    : public ExecStreamScheduler
{
    volatile bool aborted;

    SharedExecStreamGraph pGraph;

    /**
     * Finds the next consumer to execute for a given producer.
     *
     * @param graphImpl current stream graph
     * @param graphRep graph representation of current stream graph
     * @param stream currrent execution stream
     * @param edge returns edge to consumer to execute next
     * @param current returns id of consumer to execute next
     * @param skipState state to skip when looking for next consumer
     *
     * @return false if reached sink vertex, else true
     */
    bool findNextConsumer(
        ExecStreamGraphImpl &graphImpl,
        const ExecStreamGraphImpl::GraphRep &graphRep,
        const ExecStream &stream,
        ExecStreamGraphImpl::Edge &edge,
        ExecStreamId &current,
        ExecStreamBufState skipState);

public:
    /**
     * Constructs a new scheduler.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     *
     * @param name the name to use for tracing this scheduler
     */
    explicit DfsTreeExecStreamScheduler(
        SharedTraceTarget pTraceTarget,
        std::string name);

    virtual ~DfsTreeExecStreamScheduler();

    // implement the ExecStreamScheduler interface
    virtual void addGraph(SharedExecStreamGraph pGraph);
    virtual void removeGraph(SharedExecStreamGraph pGraph);
    virtual void start();
    virtual void setRunnable(ExecStream &stream, bool);
    virtual void abort(ExecStreamGraph &graph);
    virtual void checkAbort() const;
    virtual void stop();
    virtual ExecStreamBufAccessor &readStream(ExecStream &stream);
};

FENNEL_END_NAMESPACE

#endif

// End DfsTreeExecStreamScheduler.h
