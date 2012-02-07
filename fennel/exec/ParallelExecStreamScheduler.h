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

#ifndef Fennel_ParallelExecStreamScheduler_Included
#define Fennel_ParallelExecStreamScheduler_Included

#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/synch/ThreadPool.h"
#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/common/FennelExcn.h"

#include <hash_map>
#include <deque>
#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

class ExecStreamGraphImpl;
class ParallelExecStreamScheduler;
class ThreadTracker;

/**
 * ParallelExecTask represents a task submitted
 * to ParallelExecStreamScheduler's thread pool.
 */
class FENNEL_EXEC_EXPORT ParallelExecTask
{
    ParallelExecStreamScheduler &scheduler;
    ExecStream *pStream;

public:
    explicit ParallelExecTask(
        ParallelExecStreamScheduler &scheduler,
        ExecStream *pStream);

    inline ExecStreamId getStreamId() const
    {
        return pStream->getStreamId();
    }

    void execute();
};

/**
 * ParallelExecResult represents the result of a task submitted
 * to ParallelExecStreamScheduler's thread pool.
 */
class FENNEL_EXEC_EXPORT ParallelExecResult
{
    ExecStreamId streamId;
    ExecStreamResult rc;

public:
    explicit ParallelExecResult(
        ExecStreamId streamId,
        ExecStreamResult rc);

    inline ExecStreamId getStreamId() const
    {
        return streamId;
    }

    inline ExecStreamResult getResultCode() const
    {
        return rc;
    }
};

/**
 * ParallelExecStreamScheduler is a parallel implementation of the
 * ExecStreamScheduler interface.  For more information, see <a
 * href="http://pub.eigenbase.org/wiki/FennelParallelExecStreamScheduler">the
 * design doc in Eigenpedia</a>.
 *
 * @author John Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ParallelExecStreamScheduler
    : public ExecStreamScheduler, public SynchMonitoredObject
{
    enum StreamState
    {
        SS_SLEEPING,
        SS_RUNNING,
        SS_INHIBITED
    };

    struct StreamStateMapEntry
    {
        StreamState state;
        int inhibitionCount;
    };

    enum ManagerState {
        MGR_RUNNING,
        MGR_STOPPING,
        MGR_STOPPED
    };

    typedef std::hash_map<ExecStreamId, StreamStateMapEntry>
        StreamStateMap;
    typedef std::deque<ExecStreamId> InhibitedQueue;

    friend class ParallelExecTask;

    SharedExecStreamGraph pGraph;

    ThreadPool<ParallelExecTask> threadPool;
    std::deque<ParallelExecResult> completedQueue;

    ThreadTracker &threadTracker;

    StreamStateMap streamStateMap;
    ManagerState mgrState;

    InhibitedQueue inhibitedQueue;
    InhibitedQueue transitQueue;
    LocalCondition sentinelCondition;

    uint degreeOfParallelism;

    boost::scoped_ptr<FennelExcn> pPendingExcn;

    void tryExecuteManager();
    void executeManager();
    void tryExecuteTask(ExecStream &);
    void executeTask(ExecStream &);
    bool addToQueue(ExecStreamId streamId);
    void signalSentinel(ExecStreamId sentinelId);
    void retryInhibitedQueue();
    void processCompletedTask(ParallelExecResult const &task);
    inline bool isInhibited(ExecStreamId streamId);
    inline void alterNeighborInhibition(ExecStreamId streamId, int delta);

public:
    /**
     * Constructs a new scheduler.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     *
     * @param name the name to use for tracing this scheduler
     *
     * @param threadTracker tracker to use for threads created during
     * parallelization
     *
     * @param degreeOfParallelism number of threads to run
     */
    explicit ParallelExecStreamScheduler(
        SharedTraceTarget pTraceTarget,
        std::string name,
        ThreadTracker &threadTracker,
        uint degreeOfParallelism);

    virtual ~ParallelExecStreamScheduler();

    // implement the ExecStreamScheduler interface
    virtual void addGraph(SharedExecStreamGraph pGraph);
    virtual void removeGraph(SharedExecStreamGraph pGraph);
    virtual void start();
    virtual void setRunnable(ExecStream &stream, bool);
    virtual void makeRunnable(ExecStream &stream);
    virtual void abort(ExecStreamGraph &graph);
    virtual void checkAbort() const;
    virtual void stop();
    virtual ExecStreamBufAccessor &readStream(ExecStream &stream);
    virtual void createBufferProvisionAdapter(
        ExecStreamEmbryo &embryo);
    virtual uint getDegreeOfParallelism();
};

FENNEL_END_NAMESPACE

#endif

// End ParallelExecStreamScheduler.h
