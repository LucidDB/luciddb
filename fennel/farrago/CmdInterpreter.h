/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_CmdInterpreter_Included
#define Fennel_CmdInterpreter_Included

#include "fennel/farrago/JniUtil.h"
#include "fennel/farrago/Fem.h"
#include "fennel/synch/StatsTimer.h"
#include "fennel/common/FileStatsTarget.h"
#include "fennel/ftrs/BTreeExecStream.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class TraceTarget;
class JavaTraceTarget;
class JavaErrorTarget;
class BTreeDescriptor;

/**
 * CmdInterpreter interprets command objects received via JNI from Farrago.  It
 * implements FemVisitor in order to process commands; a visit on a command
 * results in its execution.
 */
class FENNEL_FARRAGO_EXPORT CmdInterpreter
    : public boost::noncopyable, virtual public FemVisitor
{
public:
    /**
     * Handle type for a database.
     */
    struct DbHandle
    {
        SharedDatabase pDb;
        boost::shared_ptr<TraceTarget> pTraceTarget;
        StatsTimer statsTimer;
        SharedExecStreamGovernor pResourceGovernor;

        // TODO:  parameterize these
        explicit DbHandle()
            : statsTimer(500)
        {
        }

        virtual ~DbHandle();            // make class polymorphic
    };

    /**
     * Handle type which combines a database with a txn.
     */
    struct TxnHandle
    {
        SharedDatabase pDb;
        SharedLogicalTxn pTxn;
        SharedFtrsTableWriterFactory pFtrsTableWriterFactory;
        bool readOnly;
        SharedExecStreamGovernor pResourceGovernor;
        /**
         * If the database supports snapshots, the snapshot segment associated
         * with the current transaction
         */
        SharedSegment pSnapshotSegment;
        /**
         * If the database supports snapshots, the snapshot segment associated
         * with the current transaction that is used in cases where reads
         * ignore uncommitted data, including those created by the current
         * transaction
         */
        SharedSegment pReadCommittedSnapshotSegment;

        explicit TxnHandle()
            : readOnly(false)
            {
            }

        virtual ~TxnHandle();           // make class polymorphic
    };

    struct StreamGraphHandle
        : public BTreeOwnerRootMap
    {
        SharedExecStreamFactory pExecStreamFactory;
        SharedExecStreamGraph pExecStreamGraph;
        SharedExecStreamScheduler pScheduler;
        TxnHandle *pTxnHandle;
        /**
         * Segment associated with the stream graph
         */
        SharedSegment pSegment;
        /**
         * Segment associated with the stream graph that only reads
         * committed data, if the underlying segment supports this
         */
        SharedSegment pReadCommittedSegment;
        // a global ref to the FarragoRuntimeContext
        jobject javaRuntimeContext;

        explicit StreamGraphHandle()
            : pTxnHandle(0), javaRuntimeContext(0)
            {
            }

        ~StreamGraphHandle();

        // implement BTreeOwnerRootMap
        virtual PageId getRoot(PageOwnerId pageOwnerId);
    };

    /**
     * Handle type for storing execution state information.
     */
    struct ExecutionHandle
    {
        /**
         * True if the command associated with this handle needs to be canceled
         */
        volatile bool aborted;
    };

    /**
     * Pointer to the execution handle associated with a command.  NULL if
     * there is no handle.
     */
    ExecutionHandle *pExecHandle;

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
    static TxnId getCsn(SharedProxyCsnHandle);

    virtual DbHandle *newDbHandle();    /// factory method
    virtual TxnHandle *newTxnHandle();  /// factory method
    void deleteDbHandle(DbHandle *);

    void setDbHandle(SharedProxyDbHandle, DbHandle *);
    void setTxnHandle(SharedProxyTxnHandle, TxnHandle *);
    void setStreamGraphHandle(
        SharedProxyStreamGraphHandle, StreamGraphHandle *);
    void setExecStreamHandle(SharedProxyStreamHandle, ExecStream *);
    void setSvptHandle(
        SharedProxySvptHandle, SavepointId);
    void setCsnHandle(SharedProxyCsnHandle, TxnId);

    void getBTreeForIndexCmd(ProxyIndexCmd &, PageId, BTreeDescriptor &);
    void dropOrTruncateIndex(
        ProxyCmdDropIndex &cmd, bool drop);

    virtual JavaTraceTarget *newTraceTarget();  /// factory method

    /**
     * Executes a begin txn command.
     *
     * @param cmd the command
     * @param readOnly true if the txn is read-only
     * @param csn if non-null and snapshots are enabled, the commit sequence
     * number for reads
     */
    void beginTxn(ProxyBeginTxnCmd &cmd, bool readOnly, TxnId csn);

    // Per-command overrides for FemVisitor; add new commands here
    virtual void visit(ProxyCmdCreateExecutionStreamGraph &);
    virtual void visit(ProxyCmdPrepareExecutionStreamGraph &);
    virtual void visit(ProxyCmdCreateStreamHandle &);
    virtual void visit(ProxyCmdCreateIndex &);
    virtual void visit(ProxyCmdTruncateIndex &);
    virtual void visit(ProxyCmdDropIndex &);
    virtual void visit(ProxyCmdVerifyIndex &);
    virtual void visit(ProxyCmdOpenDatabase &);
    virtual void visit(ProxyCmdCloseDatabase &);
    virtual void visit(ProxyCmdCheckpoint &);
    virtual void visit(ProxyCmdSetParam &);
    virtual void visit(ProxyCmdBeginTxn &);
    virtual void visit(ProxyCmdBeginTxnWithCsn &);
    virtual void visit(ProxyCmdSavepoint &);
    virtual void visit(ProxyCmdCommit &);
    virtual void visit(ProxyCmdRollback &);
    virtual void visit(ProxyCmdGetTxnCsn &);
    virtual void visit(ProxyCmdGetLastCommittedTxnId &);
    virtual void visit(ProxyCmdAlterSystemDeallocate &);
    virtual void visit(ProxyCmdVersionIndexRoot &);
    virtual void visit(ProxyCmdInitiateBackup &);
    virtual void visit(ProxyCmdCompleteBackup &);
    virtual void visit(ProxyCmdAbandonBackup &);
    virtual void visit(ProxyCmdRestoreFromBackup &);

public:
    /**
     * Interprets the given command object.
     *
     * @param cmd the ProxyCmd to be executed
     *
     * @return result handle if any
     */
    virtual int64_t executeCommand(ProxyCmd &cmd);

    static inline StreamGraphHandle &getStreamGraphHandleFromLong(jlong);
    static inline ExecStream &getExecStreamFromLong(jlong);
    static inline TxnHandle &getTxnHandleFromLong(jlong);
    static inline ExecutionHandle &getExecutionHandleFromLong(jlong);
    static inline jobject getObjectFromLong(jlong jHandle);

    /**
     * Reads the Java representation of a TupleDescriptor.
     *
     * @param tupleDesc target TupleDescriptor
     *
     * @param javaTupleDesc Java proxy representation
     *
     * @param typeFactory factory for resolving type ordinals
     */
    static void readTupleDescriptor(
        TupleDescriptor &tupleDesc,
        ProxyTupleDescriptor &javaTupleDesc,
        StoredTypeDescriptorFactory const &typeFactory);

    /**
     * Reads the Java representation of a TupleProjection
     *
     * @param tupleProj target TupleProjection
     *
     * @param pJavaTupleProj Java representation
     */
    static void readTupleProjection(
        TupleProjection &tupleProj,
        SharedProxyTupleProjection pJavaTupleProj);

    static SharedErrorTarget newErrorTarget(
        jobject fennelJavaErrorTarget);
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

inline ExecStream &CmdInterpreter::getExecStreamFromLong(jlong jHandle)
{
    return *reinterpret_cast<ExecStream *>(jHandle);
}

inline CmdInterpreter::TxnHandle &CmdInterpreter::getTxnHandleFromLong(
    jlong jHandle)
{
    return *reinterpret_cast<TxnHandle *>(jHandle);
}

inline CmdInterpreter::ExecutionHandle
&CmdInterpreter::getExecutionHandleFromLong(
    jlong jHandle)
{
    return *reinterpret_cast<ExecutionHandle *>(jHandle);
}

// The following macros are used for tracing the JniUtil handle count.
// They are defined here to allow for the allocation of these handle
// types from other locations while still deallocating them in the
// handle class destructors (handle count tracing depends on the handle
// type string being the same at allocation and deallocation time).
#define DBHANDLE_TRACE_TYPE_STR ("DbHandle")
#define TXNHANDLE_TRACE_TYPE_STR ("TxnHandle")
#define STREAMGRAPHHANDLE_TRACE_TYPE_STR ("StreamGraphHandle")
#define EXECHANDLE_TRACE_TYPE_STR ("ExecutionHandle")


FENNEL_END_NAMESPACE

#endif

// End CmdInterpreter.h
