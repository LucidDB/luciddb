/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_CmdInterpreter_Included
#define Fennel_CmdInterpreter_Included

#include "fennel/farrago/JniUtil.h"
#include "fennel/farrago/Fem.h"
#include "fennel/synch/StatsTimer.h"
#include "fennel/common/FileStatsTarget.h"
#include "fennel/xo/BTreeTupleStream.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class TraceTarget;
class BTreeDescriptor;

/**
 * CmdInterpreter interprets command objects received via JNI from Farrago.  It
 * implements FemVisitor in order to process commands; a visit on a command
 * results in its execution.
 */
class CmdInterpreter : public boost::noncopyable, virtual public FemVisitor
{
public:
    /**
     * Handle type for a database.
     */
    struct DbHandle 
    {
        SharedDatabase pDb;
        boost::shared_ptr<TraceTarget> pTraceTarget;
        FileStatsTarget statsTarget;
        StatsTimer statsTimer;

        // TODO:  parameterize these
        explicit DbHandle()
            : statsTarget("/tmp/fennel.stats"),
              statsTimer(statsTarget,500)
        {
        }
    };
    
    /**
     * Handle type which combines a database with a txn.
     */
    struct TxnHandle 
    {
        SharedDatabase pDb;
        SharedLogicalTxn pTxn;
        SharedTableWriterFactory pTableWriterFactory;
    };

    struct StreamGraphHandle : public BTreeRootMap
    {
        TxnHandle *pTxnHandle;
        jobject javaRuntimeContext;
        virtual PageId getRoot(PageOwnerId pageOwnerId);
        virtual SharedExecutionStreamGraph getGraph() = 0;
    };

    struct TupleStreamGraphHandle : public StreamGraphHandle
    {
    private:
        SharedTupleStreamGraph pGraph;
    public:
        void setTupleStreamGraph(SharedTupleStreamGraph pGraph);
        SharedExecutionStreamGraph getGraph();
        SharedTupleStreamGraph getTupleStreamGraph();
    };
    
protected:
    /**
     * Handle to be returned by current command, or 0 if none.  Have to do it
     * this way since visit returns void; note that this makes this class
     * stateful (each thread needs its own instance).
     */
    int64_t resultHandle;

    static DbHandle *getDbHandle(SharedProxyDbHandle);
    static TxnHandle *getTxnHandle(SharedProxyTxnHandle);
    static StreamGraphHandle *getStreamGraphHandle(
        SharedProxyStreamGraphHandle);
    static SavepointId getSavepointId(SharedProxySvptHandle);

    void setDbHandle(SharedProxyDbHandle,DbHandle *);
    void setTxnHandle(SharedProxyTxnHandle,TxnHandle *);
    void setStreamGraphHandle(SharedProxyStreamGraphHandle,StreamGraphHandle *);
    void setStreamHandle(SharedProxyStreamHandle,ExecutionStream *);
    void setSvptHandle(
        SharedProxySvptHandle,SavepointId);

    void getBTreeForIndexCmd(ProxyIndexCmd &,PageId,BTreeDescriptor &);

    // Per-command overrides for FemVisitor; add new commands here
    virtual void visit(ProxyCmdCreateExecutionStreamGraph &);
    virtual void visit(ProxyCmdPrepareExecutionStreamGraph &);
    virtual void visit(ProxyCmdCreateStreamHandle &);
    virtual void visit(ProxyCmdCreateIndex &);
    virtual void visit(ProxyCmdTruncateIndex &);
    virtual void visit(ProxyCmdDropIndex &);
    virtual void visit(ProxyCmdOpenDatabase &);
    virtual void visit(ProxyCmdCloseDatabase &);
    virtual void visit(ProxyCmdCheckpoint &);
    virtual void visit(ProxyCmdBeginTxn &);
    virtual void visit(ProxyCmdSavepoint &);
    virtual void visit(ProxyCmdCommit &);
    virtual void visit(ProxyCmdRollback &);

public:
    /**
     * Interpret the given command object.
     *
     * @param cmd the ProxyCmd to be executed
     *
     * @return result handle if any
     */
    virtual int64_t executeCommand(ProxyCmd &cmd);

    static inline StreamGraphHandle &getStreamGraphHandleFromLong(jlong);
    static inline ExecutionStream &getStreamFromLong(jlong);
    static inline TxnHandle &getTxnHandleFromLong(jlong);
    static inline jobject getObjectFromLong(jlong jHandle);

    // override JniProxyVisitor
    void *getLeafPtr();
    const char *getLeafTypeName();
};

inline jobject CmdInterpreter::getObjectFromLong(jlong jHandle)
{
    jobject *pGlobalRef = reinterpret_cast<jobject *>(jHandle);
    return *pGlobalRef;
}

inline CmdInterpreter::StreamGraphHandle &
CmdInterpreter::getStreamGraphHandleFromLong(jlong jHandle)
{
    return *reinterpret_cast<StreamGraphHandle *>(jHandle);
}

inline ExecutionStream &CmdInterpreter::getStreamFromLong(jlong jHandle)
{
    return *reinterpret_cast<ExecutionStream *>(jHandle);
}

inline CmdInterpreter::TxnHandle &CmdInterpreter::getTxnHandleFromLong(
    jlong jHandle)
{
    return *reinterpret_cast<TxnHandle *>(jHandle);
}

FENNEL_END_NAMESPACE

#endif

// End CmdInterpreter.h
